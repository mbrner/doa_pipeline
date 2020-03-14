"""

"""
import dataclasses
from typing import (
    Any,
)
ACTIVE_DAGS = []
DETACHED = 'DETACHED'


class Node:
    def __init__(self, name, payload=None, dag=None):
        self.name = name
        self.payload = payload
        if dag is None:
            try:
                dag = ACTIVE_DAGS[-1]
            except IndexError:
                dag = None
        self.dag = dag
        if self.dag is None or self.dag == False:
            self._is_detached = True
            self._incoming_edges = None
            self._outgoing_edges = None
            self.dag = False
        else:
            if not isinstance(self.dag, DAG):
                raise TypeError('"dag" has to be of type DAG.')
            self._is_detached = False
            self._incoming_edges = set()
            self._outgoing_edges = set()
            self.dag.add_node(self)

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
        if self.is_detached:
            s = f'{self.name} (DETACHED)'
        else:
            s = f'{self.name} ({self.dag.name})'
        if self.payload is not None:
            s += f' (Payload: {self.payload})'
        return s

    def __hash__(self):
        return hash((self.dag, self.name))

    def __eq__(self, other):
        return hash(self) == hash(other)
    
    def __repr__(self):
        return str(self)

    def __rshift__(self, other):
        edge = self.dag.add_edge(self, other)
        return self, edge, other

    def __lshift__(self, other):
        edge = self.dag.add_edge(other, self)
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



class DAG:
    def __init__(self, name: str):
        self.name = name
        self.nodes = set()
        self._sorted_nodes = None
        self._components = None

    def __len__(self):
        return len(self.nodes)

    def __enter__(self) -> "DAG":
        ACTIVE_DAGS.append(self)
        return self

    def __exit__(self, _type, _value, _tb) -> None:
        ACTIVE_DAGS.pop()

    def add_node(self, node, replace=False):
        if not node.dag or node.dag == self:
            try:
                if node.dag == self:
                    old_node = self.find(node)
                elif node.dag == False:
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
                    raise ValueError(f'Node with name: {node.name} already present in dag: {self.name}')
            self.nodes.add(node)
            node.dag = self
            self._components = None
            self._sorted_nodes = None
        else:
            raise ValueError('node is from a different DAG')

    def add_edge(self, start_node, end_node, payload=None, replace=False):
        if start_node not in self.nodes:
            raise ValueError('"start_node" is a node from a different DAG')
        if end_node not in self.nodes:
            raise ValueError('"end_node" is a node from a different DAG')
        edge = start_node.add_edge(end_node, payload=payload, replace=replace)
        self._components = None
        self._sorted_nodes = None
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
                        'payload': e.payload} for e in self.edges],
             'order': [str(hash(v)) for v in self.nodes]}
        return d

    @classmethod
    def from_dict(cls, d):
        node_lookup = {}
        with cls(d['name']) as dag:
            for h in d['order']:
                node = d['nodes'][h]
                node = Node(node['name'], node.get('payload', None))
                node_lookup[h] = node
            for edge in d['edges']:
                start_node = node_lookup[edge['start']]
                stop_node = node_lookup[edge['stop']]
                start_node.add_edge(stop_node, edge.get('payload', None))
        return dag

    @property
    def sorted_nodes(self):
        if self._sorted_nodes is None:
            self._sorted_nodes = depth_first_search(self)
            self._components = None
        return self._sorted_nodes

    @property
    def components(self):
        if self._components is None:
            self._components = get_components(self)
        return self._components


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


def get_components(dag):
    sorted_nodes = dag.sorted_nodes
    look_up = {n: set([n]) for n in sorted_nodes}
    visited = set()

    def join_components(comp_m, comp_n):
        joinded_component = comp_m.union(comp_n)
        for n_i in comp_m:
            look_up[n_i] = joinded_component
        for n_i in comp_n:
            look_up[n_i] = joinded_component

    def visit(n, prev=None):
        component_n = look_up[n]
        if prev is not None:
            component_prev = look_up[prev]
            join_components(component_n, component_prev)
        if n not in visited:
            for e in n.outgoing_edges:
                visit(e.stop, n)
        else:
            return

    for n in sorted_nodes:
        if n not in visited:
            visit(n, None)
    return look_up

                



def depth_first_search(dag):
    """
    """
    nodes = dag.nodes
    sorted_nodes = []
    temporary_mark = set()
    permanent_mark = set()
    def visit(n):
        if n in permanent_mark:
            return
        if n in temporary_mark:
            raise ValueError(f'DAG is acyclic. node={n} visited_twice.')
        temporary_mark.add(n)
        for e_i in n.outgoing_edges:
            visit(e_i.stop)
        temporary_mark.remove(n)
        permanent_mark.add(n)
        sorted_nodes.append(n)
    
    for n in nodes:#entry_nodes:
        if n not in permanent_mark:
            visit(n)
    return sorted_nodes[::-1]