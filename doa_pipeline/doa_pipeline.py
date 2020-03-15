import dataclasses
import datetime
import copy
import json
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
    cast)
import enum
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .dag import DAG, Node
from .db_utils import ArrayOfEnum, create_engine_context


ACTIVE_DOA_PIPELINES = []
DEFAULT_COLUMN_NAME_FUNC = lambda _, node_name, col: f'{node_name}_{col.name}'


class NodeStatus(enum.Enum):
    FAILED = 'F'
    WAITING = 'W'
    QUEUED = 'Q'
    RUNNING = 'R'
    SUCCESS = 'S'


class ProcessStatus(enum.Enum):
    FAILED = 'F'
    WAITING = 'W'
    QUEUED = 'Q'
    RUNNING = 'R'
    SUCCESS = 'S'
    PAUSED = 'P'




@dataclasses.dataclass
class DOANodeConfig:
    name: str
    version: str
    result_columns: List[sa.Column] = dataclasses.field(default_factory=lambda: [])
    dag_columns: Dict[str, str] = dataclasses.field(default_factory=lambda: {})

    def col(self, name, doa_datalayer=None):
        if doa_datalayer is None:
            doa_datalayer = ACTIVE_DOA_PIPELINES[-1]
        return self.dag_columns[doa_datalayer.name][name]

    def __getattr__(self, name):
        try:
            return self.col(name)
        except (IndexError, KeyError):
            raise AttributeError(f"'DOANodeConfig' has no ttribute '{name}'")

@dataclasses.dataclass
class ProcessingContext:
    config: DOANodeConfig
    dag_context: Dict
    results: Dict = dataclasses.field(default_factory=lambda: {})


class DOADataLayer:
    column_name_func = DEFAULT_COLUMN_NAME_FUNC
    context_dump = json.dumps
    context_load = json.loads

    def __init__(self, name):
        self.name = name
        self.dag = DAG(name)
        self.columns = {}
        self.metadata = sa.MetaData()
        self._table = None
        self._engine = None
        self._active_session = None
        
    def create_node(self, doa_node_cfg):
        name = doa_node_cfg.name
        node_columns = {}
        for col in doa_node_cfg.result_columns:
            new_col = copy.copy(col)
            new_col.name = DOADataLayer.column_name_func(name, self.name, col)
            node_columns[col.name] = new_col
        node = Node(name,
                    payload={'version': doa_node_cfg.version,
                             'column_names': [node_columns[c.name].name for c in doa_node_cfg.result_columns]},
                    dag=self.dag)
        self.columns[node] = node_columns
        doa_node_cfg.dag_columns[self.name] = node_columns
        return node

    def build_db_table(self) -> sa.Table:
        sorted_nodes = self.dag.sorted_nodes
        self.node_order = {n.name.upper(): i for i, n in  enumerate(sorted_nodes)}
        self.node_order['CONTEXT'] = -1
        self.update_enum = enum.Enum('Update', self.node_order)
        node_status_default = ProcessStatus.SUCCESS.value + (ProcessStatus.WAITING.value * (len(self.node_order) - 1))
        table_cols = [
            sa.Column('id',
                      sa.Integer,
                      primary_key=True),
            sa.Column('status',
                      sa.CHAR,
                      server_default=ProcessStatus.WAITING.value),
            sa.Column('create',
                      sa.DateTime,
                      server_default=sa.sql.func.now()),
            sa.Column('dag',
                      sa.Text,
                      server_default=DOADataLayer.context_dump(self.dag.to_dict())),
            sa.Column('context',
                      sa.Text),
            sa.Column('node_status',
                      sa.String,
                      server_default=node_status_default),
            sa.Column('last_update_time',
                      sa.DateTime,
                      server_default=sa.sql.func.now(),
                      onupdate=sa.sql.func.now()),
            sa.Column('last_update_node',
                      sa.Enum(self.update_enum),
                      server_default=self.update_enum(-1).name),
            sa.Column('error_traceback',
                      sa.Text,
                      server_default='')]
        for node in sorted_nodes:
            table_cols.extend([*self.columns.get(node, {}).values()])
        return sa.Table(f'{self.name}', self.metadata, *table_cols, extend_existing=True)

    def __call__(self, engine) -> "DOADataLayer":
        if self._table is None:
            self._table = self.build_db_table()
        if isinstance(engine, sa.engine.base.Engine):
            self._engine = engine
        elif isinstance(engine, str):
            self._engine = sa.create_engine(engine)
        else:
            raise ValueError('Provide a direct Engine or an adress that passed to sa.create_engine(...)')
        self.metadata.create_all(self._engine, checkfirst=True)
        self.session_scope = create_engine_context(self._engine)
        return self

    @property
    def table(self):
        if self._table is None:
            self._table = self.build_db_table()
        return self._table            

    def __enter__(self) -> sa.orm.session.Session:
        ACTIVE_DOA_PIPELINES.append(self)
        self._active_session_scope = self.session_scope()
        self._active_session = self._active_session_scope.__enter__()
        return self._active_session

    def __exit__(self, _type, _value, _tb):
        ACTIVE_DOA_PIPELINES.pop()
        self._active_session_scope.__exit__(_type, _value, _tb)
        self._active_session = None
        self._active_session_scope = None

    def query_for_work(self, node_cfg):
        table = self._table
        if self._active_session is None:
            raise ValueError('Use or with DOADataLayer(engine=) before querying the database')
        node = self.dag.find(node_cfg.name)
        upstream_nodes = [getattr(self.update_enum, e.start.name.upper()).value
                          for e in node.incoming_edges]
        like_str = 'S'
        own_idx = getattr(self.update_enum, node.name.upper()).value
        for i in range(len(self.dag)):
            if i == own_idx:
                like_str += 'W'
            elif i in upstream_nodes:
                like_str += 'S'
            else:
                like_str += '*'
        sq = self._active_session.query(table.c.last_update_time) \
            .order_by(table.c.last_update_time.desc()).limit(1).with_for_update()
        q = sa.update(table)  \
            .where(sa.and_(table.c.last_update_time == sq.as_scalar(),
                           table.c.node_status.like(like_str),
                           table.c.status == ProcessStatus.WAITING.value))
        res = self._active_session.execute(q)
        if res is None:
            raise ValueError
        else:
            print(type(res))
            entry = res.fetchone()
            print(entry)
        


    def add_process(self, context={}):
        if self._active_session is None:
            raise ValueError('Use or with DOADataLayer(engine=) before adding a process to the database.')
        q = self.table.insert().values(context=json.dumps(context))
        self._active_session.execute(q)
        


    def col(self, node_cfg, col, check_added=True):
        not_found_err = AttributeError(f'No column "{col}" found!')
        if check_added:
            if self.name not in node_cfg.dag_columns.keys():
                raise ValueError('node_cfg not used for this DAG')
        if isinstance(col, int):
            try:
                return getattr(self.table, [*self.columns[node_cfg.name].values()][col])
            except (AttributeError, IndexError):
                raise not_found_err
        elif isinstance(col, str):
            try:
                return getattr(self.table, col)
            except AttributeError:
                try:
                    col = [c for c in node_cfg.columns if c.name == col][0]
                    name = self.column_name_func(node_cfg.name, self.dag.name, col)
                    return getattr(self.table, name)
                except (AttributeError, IndexError):
                    raise not_found_err
        elif isinstance(col, sa.Column):
            name = self.column_name_func(node_cfg.name, self.dag.name, col)
            try:
                return getattr(self.table, name)
            except AttributeError:
                raise not_found_err
        else:
            raise TypeError('"col" has to be int, str or sa.Column')


        