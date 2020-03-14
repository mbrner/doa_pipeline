import dataclasses
import datetime
import copy
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

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .dag import DAG, Node
from .db_utils import ArrayOfEnum, create_engine_context


class NodeStatus(enum.Enum):
    FAILED = -1
    PENDING = 0
    QUEUED = 1
    RUNNING = 2
    FINISHED = 3


class ProcessStatus(enum.Enum):
    FAILED = -1
    PENDING = 0
    QUEUED = 1
    RUNNING = 2
    FINISHED = 3
    PAUSED = 10


@dataclasses.dataclass
class DOANodeConfig:
    name: str
    version: str
    result_columns: List[sa.Column] = dataclasses.field(default_factory=lambda: [])




class DOADataLayer:
    column_name_func = lambda _, node_name, col: f'{node_name}_{col.name}'

    def __init__(self, name):
        self.name = name
        self.dag = DAG(name)
        self.columns = {}
        self.metadata = sa.MetaData()
        self._table = None
        self._engine = None
        self._session_scope = None
        
    def create_node(self, doa_node_cfg):
        name = doa_node_cfg.name
        node_columns = []
        for col in doa_node_cfg.result_columns:
            new_col = copy.copy(col)
            new_col.name = self.column_name_func(name, self.dag.name, col)
            node_columns.append(new_col)
        node = Node(name,
                    payload={'version': doa_node_cfg.version,
                             'column_names': [c.name for c in node_columns]},
                    dag=self.dag)
        self.columns[node] = node_columns
        return node

    def build_db_table(self) -> sa.Table:
        sorted_nodes = self.dag.sorted_nodes
        node_order = {n.name.upper(): i for i, n in  enumerate(sorted_nodes)}
        node_order['INITIALIZED'] = -1
        self.update_enum = enum.Enum('Update', node_order)
        table_cols = [
            sa.Column('id',
                      sa.Integer,
                      primary_key=True),
            sa.Column('dag',
                      postgresql.JSON,
                      default=self.dag.to_dict()),
            sa.Column('context',
                      sa.Text,
                      default={}),
            sa.Column('node_status',
                      ArrayOfEnum(sa.Enum(NodeStatus)),
                      default=[ProcessStatus.PENDING for _ in range(len(node_order))]),
            sa.Column('last_update_time',
                      sa.DateTime,
                      server_default=sa.text('NOW()')),
            sa.Column('last_update_node',
                      sa.Enum(self.update_enum),
                      default=lambda: self.update_enum(-1)),
            sa.Column('final_result',
                      sa.Text,
                      default='')]
        for node in sorted_nodes:
            table_cols.extend(self.columns.get(node, []))
        return sa.Table(f'{self.name}', self.metadata, *table_cols)

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
        return self

    @property
    def table(self):
        if self._table is None:
            self._table = self.build_db_table()
        return self._table            

    def __enter__(self) -> sa.orm.session.Session:
        self._session_scope = create_engine_context(self._engine)
        return self._session_scope.__enter__()

    def __exit__(self, _type, _value, _tb):
        self._session_scope.__exit__(_type, _value, _tb)
        self._session_scope = None

    def query_for_work(self, node_cfg):
        if self._session_scope is None:
            raise ValueError('Use DOADataLayer.connect(eingine=...) or with DOADataLayer.-')
        node = self.dag.find(node_cfg.name)
        upstream_nodes = [getattr(self.update_enum, e.start.name.upper()).value
                          for e in node.incoming_edges]


    def col(self, node_cfg, col):
        not_found_err = AttributeError(f'No column "{col}" found!')
        if isinstance(col, int):
            try:
                return getattr(self.table, self.columns[node_cfg.name][col])
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


        