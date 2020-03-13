"""

"""
import dataclasses
from typing import (
    Any,
)
ACTIVE_DAQS = []
DETACHED = 'DETACHED'


class Node:
    def __init__(self, name, payload=None, daq=None):
        self.name = name
        self.payload = payload
        if daq is None:
            try:
                daq = ACTIVE_DAQS[-1]
            except IndexError:
                daq = None
        self.daq = daq
        
        if not self.daq:
            self._is_detached = True
            self._incoming_edges = None
            self._outgoing_edges = None
            self.daq = False
        else:
            if not isinstance(self.daq, DAQ):
                raise TypeError('"daq" has to be of type DAQ.')
            self._is_detached = False
            self._incoming_edges = set()
            self._outgoing_edges = set()
            self.daq.add_node(self)

    @property
    def is_detached(self):
        return self._is_detached

    @property
    def incoming_edges(self):
        if self.is_detached:
            raise ValueError('Detached nodes do not have any edges.')
        else:
            return self._incoming_edges
        
    @property
    def outgoing_edges(self):
        if self.is_detached:
            raise ValueError('Detached nodes do not have any edges.')
        else:
            return self._outgoing_edges

    def add_edge(self, end_node, payload=None, replace=False):
        if self.is_detached:
            raise ValueError('Edges canot be added to detached nodes.')
        if self == end_node:
            return ValueError('Self referencing edges not allowed')
        edge = Edge(start=self, stop=end_node, payload=payload)
        if edge in self.outgoing_edges:
            if replace:
                self.outgoing_edges.remove(edge)
                end_node.incoming_edges.remove(edge)
            else:
                raise ValueError(f'Edge between {self} and {end_node} already present')
        self.outgoing_edges.add(edge)
        end_node.incoming_edges.add(edge)
        return edge


    def __str__(self):
        s = f'{self.name} ({self.daq.name})'
        if self.payload is not None:
            s += f' (Payload: {self.payload})'
        return s

    def __hash__(self):
        return hash((self.daq, self.name))

    def __eq__(self, other):
        return hash(self) == hash(other)
    
    def __repr__(self):
        return str(self)

    def __rshift__(self, other):
        edge = self.daq.add_edge(self, other)
        return self, edge, other

    def __lshift__(self, other):
        edge = self.daq.add_edge(other, self)
        return self, edge, other

@dataclasses.dataclass
class Edge:
    start: Node
    stop: Node
    payload: Any = None

    def __hash__(self):
        return hash((self.start, self.stop))

    def __eq__(self, other):
        return hash(self) == hash(other)



class DAQ:
    def __init__(self, name: str):
        self.name = name
        self.nodes = set()

    def __enter__(self) -> "DAQ":
        ACTIVE_DAQS.append(self)
        return self


    def __exit__(self, _type, _value, _tb) -> None:
        ACTIVE_DAQS.pop()

    def add_node(self, node, replace=False):
        if not node.daq or node.daq == self:
            try:
                if node.daq == self:
                    old_node = self.find(node)
                elif node.daq == False:
                    old_node = self.find(node.name)
                else:
                    raise RuntimeError('Unexpected case!')
            except ValueError:
                old_node = None
            if old_node is not None:
                if replace:
                    for inc_edge in old_node.incoming_edges:
                        inc_edge.stop = node
                    for out_edge in old_node.outgoing_edges:
                        out_edge.start = node
                    self.nodes.remove(old_node)
                else:
                    raise ValueError(f'Node with name: {node.name} already present in daq: {self.name}')
            self.nodes.add(node)
            node.daq = self
        else:
            raise ValueError('node is from a different DAQ')

    def add_edge(self, start_node, end_node, payload=None, replace=False):
        if start_node not in self.nodes:
            raise ValueError('"start_node" is a node from a different DAQ')
        if end_node not in self.nodes:
            raise ValueError('"end_node" is a node from a different DAQ')
        edge = start_node.add_edge(end_node, payload=payload, replace=replace)
        return edge

    @property
    def edges(self):
        edges = set()
        for node in self.nodes:
            edges = edges.union(node.outgoing_edges)
        return edges


    def to_dict(self):
        d = {'name': self.name,
             'nodes': {str(hash(v)): {'name': v.name, 'payload': v.payload} for v in self.nodes},
             'edges': [{'start': str(hash(e.start)),
                        'stop': str(hash(e.stop)),
                        'payload': e.payload} for e in self.edges]}
        return d

    @classmethod
    def from_dict(cls, d):
        node_lookup = {}
        with cls(d['name']) as daq:
            for h, node in d['nodes'].items():
                node = Node(node['name'], node.get('payload', None))
                node_lookup[h] = node
            for edge in d['edges']:
                start_node = node_lookup[edge['start']]
                stop_node = node_lookup[edge['stop']]
                start_node.add_edge(stop_node, edge.get('payload', None))
        return daq

    @property
    def sorted_nodes(self):
        return depth_first_search(self)

    def find(self, node):
        nodes_list = list([*self.nodes])
        if isinstance(node, Node):
            return nodes_list[nodes_list.index(node)]
        elif isinstance(node, str):
            return nodes_list[[v.name for v in nodes_list].index(node)]
        else:
            raise TypeError("Sought-after node has to be of type 'Node' or 'str' (name of the node)")

    def __hash__(self):
        return hash(self.name)



def depth_first_search(daq):
    """
    """
    nodes = daq.nodes
    sorted_nodes = []
    temporary_mark = set()
    permanent_mark = set()
    #entry_nodes = set(n for n in nodes if len(n.incoming_edges) == 0)
    def visit(n):
        if n in permanent_mark:
            return
        if n in temporary_mark:
            raise ValueError(f'DAQ is acyclic. node={n} visited_twice.')
        temporary_mark.add(n)
        for e_i in n.outgoing_edges:
            visit(e_i.stop)
        temporary_mark.remove(n)
        permanent_mark.add(n)
        sorted_nodes.append(n)
    
    for n in nodes:#entry_nodes:
        if n not in permanent_mark:
            visit(n)

    #not_visited = nodes.difference(permanent_mark)
    #if len(not_visited) > 0:
        #not_visited = ', '.join(not_visited)
        #raise ValueError(f'Nodes [{not_visited}] build a/multiple cycles with no start!')
    return sorted_nodes[::-1]