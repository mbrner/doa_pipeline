import dataclasses
import json
import datetime
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
from sqlalchemy.dialects.postgresql import JSON

from .daq import DAQ, Node
from .db_utils import ArrayOfEnum


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
        self.daq = DAQ(name)
        self.columns = {}
        
    def create_node(self, doa_node_cfg):
        name = doa_node_cfg.name
        node_columns = []
        for col in doa_node_cfg.result_columns:
            new_col = copy.copy(col)
            new_col.name = self.column_name_func(name, self.daq.name, col)
            node_columns.append(new_col)
        node = Node(name,
                      payload={'version': doa_node_cfg.version,
                               'column_names': [c.name for c in node_columns]},
                      daq=self.daq)
        self.columns[node] = node_columns
        return node

    def build_db_table(self):
        sorted_nodes = self.daq.sorted_nodes
        node_order = {n.name.upper(): i for i, n in  enumerate(sorted_nodes)}
        node_order['INITIALIZED'] = -1
        update_enum = enum.Enum('Update', node_order)
        jsonified_daq = json.dumps(self.daq.to_dict())
        table_cols = [
            sa.Column('id',
                      sa.Integer,
                      primary_key=True),
            sa.Column('daq', sa.Text, default=jsonified_daq),
            sa.Column('context', sa.Text, default=json.dumps({})),
            sa.Column('node_status',
                      ArrayOfEnum(sa.Enum(NodeStatus)),
                      default=[ProcessStatus.PENDING
                               for _ in range(len(node_order))]),
            sa.Column('last_update_time',
                      sa.DateTime,
                      default=lambda: datetime.datetime.now()),
            sa.Column('last_update_node',
                      sa.Enum(update_enum),
                      default=lambda: update_enum(-1)),
            sa.Column('final_result', sa.Text, default='')]
        for node in sorted_nodes:
            table_cols.extend(self.columns.get(node, []))

        metadata = sa.MetaData()
        process_table = sa.Table(f'{self.name}', metadata, *table_cols)

        return process_table

    def __enter__(self) -> "DAQ":
        return self.daq.__enter__()

    def __exit__(self, _type, _value, _tb) -> None:
        return self.daq.__exit__(_type, _value, _tb)