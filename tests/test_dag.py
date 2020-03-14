import pytest
import json

from doa_pipeline.dag import DAG, Node, Edge, DETACHED


def test_dag_build():
    dag = DAG('TEST')
    with dag:
        a, e_ab, b = Node('a') >> Node('b')
        c, e_dc, d = Node('c') << Node('d')
        f = Node('f')
        f >> a
        f >> d
        g = Node('g')
        dag.add_edge(c, g)
        dag.add_edge(b, g)
    assert dag.find(a) is not None
    assert dag.find('a') is not None
    with pytest.raises(ValueError):
        dag.find('q')
    sorted_nodes = dag.sorted_nodes
    sorted_node_names = [n.name for n in sorted_nodes]
    assert sorted_node_names.index('a') < sorted_node_names.index('b')
    assert sorted_node_names.index('c') > sorted_node_names.index('d')
    assert sorted_node_names[0] == 'f'
    assert sorted_node_names[-1] == 'g'


    with DAG('TEST2') as dag_2:
        a_dag_2 = Node('a')
        h_dag_2 = Node('h')
        a_dag_2_replacement = Node('a', payload='second add', dag=False)
        dag_2.add_edge(a_dag_2, h_dag_2)
        with pytest.raises(ValueError):
            dag_2.add_node(a_dag_2_replacement)
        dag_2.add_node(a_dag_2_replacement, replace=True)

    with pytest.raises(ValueError):
        dag_2.find(a)

    with dag:
        with pytest.raises(ValueError):
            g >> Node('a')
            g >> a_dag_2_replacement



def test_dag_components():
    dag = DAG('TEST')
    with dag:
        _, _, a_2 = Node('a_1') >> Node('a_2')
        _, _, b_2 = Node('b_1') >> Node('b_2')
    lookup = dag.components
    assert len(set([tuple(c) for c in lookup.values()])) == 2
    with dag:
        _, _, c = b_2 >> Node('c')
        a_2 >> c
    lookup = dag.components
    assert len(set([tuple(c) for c in lookup.values()])) == 1
    with dag:
        Node('d')
    lookup = dag.components
    assert len(set([tuple(c) for c in lookup.values()])) == 2


def test_dag_store():
    dag = DAG('TEST')
    with dag:
        a, e_ab, b = Node('a') >> Node('b')
        c, e_dc, d = Node('c') << Node('d')
        e_ab.payload = 'edge payload'
        f = Node('f', payload='node payload')
        f >> a
        f >> d
        g = Node('g')
        dag.add_edge(c, g)
        dag.add_edge(b, g)
    dag_dict = dag.to_dict()
    dag_rec = DAG.from_dict(dag_dict)
    jsonified_dag = json.dumps(dag_rec.to_dict())
    dag_json_rec = DAG.from_dict(json.loads(jsonified_dag))
    assert [n for n in dag.nodes] == [n for n in dag_rec.nodes]
    assert [n.payload for n in dag.nodes] == [n.payload for n in dag_rec.nodes]
    assert [n for n in dag.nodes] == [n for n in dag_json_rec.nodes]
    assert [n.payload for n in dag.nodes] == [n.payload for n in dag_json_rec.nodes]
