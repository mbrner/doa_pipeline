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
import traceback
import io

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
    UNKNOWN = 'U'


class ProcessStatus(enum.Enum):
    FAILED = 'F'
    WAITING = 'W'
    QUEUED = 'Q'
    RUNNING = 'R'
    SUCCESS = 'S'
    PAUSED = 'P'
    UNKNOWN = 'U'




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
    id_: int
    context: Dict
    update_enum: 'Update'
    process_status: str
    previous_process_status: str
    claimed: bool


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
        self._running_processes = {}
        
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
                      sa.String,
                      server_default=ProcessStatus.WAITING.value),
            sa.Column('started',
                      sa.DateTime,
                      server_default=sa.sql.func.now()),
            sa.Column('finished',
                      sa.DateTime,
                      server_default=None,
                      nullable=True),
            sa.Column('dag',
                      sa.Text,
                      server_default=DOADataLayer.context_dump(self.dag.to_dict())),
            sa.Column('context',
                      sa.Text),
            sa.Column('node_status',
                      sa.String,
                      server_default=node_status_default),
            sa.Column('updated_time',
                      sa.DateTime,
                      server_default=sa.sql.func.now(),
                      onupdate=sa.sql.func.now()),
            sa.Column('updated_previous_status',
                      sa.String,
                      server_default=ProcessStatus.UNKNOWN.value),
            sa.Column('updated_node',
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

    def query_for_work(self, node_cfg, claim=True) -> Union[None, ProcessingContext]:
        table = self._table
        if self._active_session is None:
            raise ValueError('Use or with DOADataLayer(engine=) before querying the database')
        node = self.dag.find(node_cfg.name)
        upstream_nodes = [getattr(self.update_enum, e.start.name.upper()).value
                          for e in node.incoming_edges]
        like_str = 'S'
        own_enum = getattr(self.update_enum, node.name.upper())
        for i in range(len(self.dag)):
            if i == own_enum.value:
                like_str += 'W'
            elif i in upstream_nodes:
                like_str += 'S'
            else:
                like_str += '_'
        sq = self._active_session.query(table.c.id, table.c.node_status, table.c.context) \
            .filter(sa.and_(table.c.node_status.like(like_str),
                            table.c.status == ProcessStatus.WAITING.value)) \
            .order_by(table.c.updated_time.desc()).limit(1)
        res = self._active_session.execute(sq).fetchone()
        if res is None:
            return None
        id_, node_status, context = res
        if claim:
            new_status = list(node_status)
            prev_status = new_status[own_enum.value + 1]
            new_status[own_enum.value + 1] = NodeStatus.RUNNING.value
            new_status = ''.join(new_status)
            q = sa.update(table) \
                .values(status=ProcessStatus.RUNNING.value,
                        node_status=new_status,
                        updated_node=own_enum.name,
                        updated_previous_status=prev_status) \
                .where(sa.and_(table.c.node_status == node_status,
                               table.c.id == res[0],
                               table.c.status == ProcessStatus.WAITING.value))
            res = self._active_session.execute(q)
            if res.rowcount == 0:
                return None
        else:
            new_status = node_status
        processing_context = ProcessingContext(
            config=node_cfg,
            id_=id_,
            process_status=new_status,
            previous_process_status=node_status,
            update_enum=own_enum,
            context=DOADataLayer.context_load(context),
            claimed=claim)
        return processing_context


    def create_result_container(self, processing_context):
        if not processing_context.claimed:
            raise ValueError('Result containers can only be created for claimed processing_contexts.')
        result_attributes = [('traceback', Union[str, None], dataclasses.field(default=None))]
        for name, c in processing_context.config.dag_columns.get(self.name, {}).items():
            try:
                python_type = c.type.python_type
            except NotImplementedError:
                python_type = Any
            result_attributes.append((name, python_type, dataclasses.field(default=None)))
        return dataclasses.make_dataclass('ResultContainer', result_attributes)
    

    def store_result(self, processing_context, result_container):
        new_status = list(processing_context.process_status)
        new_status[processing_context.update_enum.value + 1] = NodeStatus.SUCCESS.value
        new_status = ''.join(new_status)
        values = {
            'finished': datetime.datetime.now(),
            'node_status': new_status,
            'updated_previous_status': processing_context.process_status,
            'status': ProcessStatus.SUCCESS.value,
            'updated_node': processing_context.update_enum.name,
        }
        for name, c in processing_context.config.dag_columns.get(self.name, {}).items():
            values[c.name] = getattr(result_container, name)
        q = sa.update(self._table) \
                .values(**values) \
                .where(self._table.c.id == processing_context.id_)
        res = self._active_session.execute(q)

    def store_crash(self, processing_context, result_container):
        new_status = list(processing_context.process_status)
        new_status[processing_context.update_enum.value + 1] = NodeStatus.FAILED.value
        new_status = ''.join(new_status)
        q = sa.update(self._table) \
                .values(error_traceback=result_container.traceback,
                        finished=datetime.datetime.now(),
                        node_status=new_status,
                        updated_node=processing_context.update_enum.name,
                        updated_previous_status=processing_context.process_status,
                        status=ProcessStatus.FAILED.value) \
                .where(self._table.c.id == processing_context.id_)
        res = self._active_session.execute(q)

    @contextmanager
    def process(self, processing_context):
        result_container = self.create_result_container(processing_context)

        self._running_processes[processing_context.id_] = result_container
        try:
            yield result_container
        except Exception as err:
            buffer = io.StringIO()
            traceback.print_exc(file=buffer)
            result_container.traceback = buffer.getvalue()
            self.store_crash(processing_context, result_container)
        else:
            self.store_result(processing_context, result_container)
        finally:
            del self._running_processes[processing_context.id_]

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


        