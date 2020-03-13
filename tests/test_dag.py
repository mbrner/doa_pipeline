import pytest
import json

from doa_pipeline.daq import DAQ, Node, Edge, DETACHED


def test_daq_build():
    daq = DAQ('TEST')
    with daq:
        a, e_ab, b = Node('a') >> Node('b')
        c, e_dc, d = Node('c') << Node('d')
        f = Node('f')
        f >> a
        f >> d
        g = Node('g')
        daq.add_edge(c, g)
        daq.add_edge(b, g)
    assert daq.find(a) is not None
    assert daq.find('a') is not None
    with pytest.raises(ValueError):
        daq.find('q')
    sorted_nodes = daq.sorted_nodes
    sorted_node_names = [n.name for n in sorted_nodes]
    assert sorted_node_names.index('a') < sorted_node_names.index('b')
    assert sorted_node_names.index('c') > sorted_node_names.index('d')
    assert sorted_node_names[0] == 'f'
    assert sorted_node_names[-1] == 'g'


    with DAQ('TEST2') as daq_2:
        a_daq_2 = Node('a')
        h_daq_2 = Node('h')
        a_daq_2_replacement = Node('a', payload='second add', daq=False)
        daq_2.add_edge(a_daq_2, h_daq_2)
        with pytest.raises(ValueError):
            daq_2.add_node(a_daq_2_replacement)
        daq_2.add_node(a_daq_2_replacement, replace=True)

    with pytest.raises(ValueError):
        daq_2.find(a)

    with daq:
        with pytest.raises(ValueError):
            g >> Node('a')
            g >> a_daq_2_replacement


def test_daq_store():
    daq = DAQ('TEST')
    with daq:
        a, e_ab, b = Node('a') >> Node('b')
        c, e_dc, d = Node('c') << Node('d')
        e_ab.payload = 'edge payload'
        f = Node('f', payload='node payload')
        f >> a
        f >> d
        g = Node('g')
        daq.add_edge(c, g)
        daq.add_edge(b, g)
    daq_dict = daq.to_dict()
    daq_rec = DAQ.from_dict(daq_dict)
    jsonified_daq = json.dumps(daq_rec.to_dict())
    daq_json_rec = DAQ.from_dict(json.loads(jsonified_daq))
    assert [n for n in daq.nodes] == [n for n in daq_rec.nodes]
    assert [n.payload for n in daq.nodes] == [n.payload for n in daq_rec.nodes]
    assert [n for n in daq.nodes] == [n for n in daq_json_rec.nodes]
    assert [n.payload for n in daq.nodes] == [n.payload for n in daq_json_rec.nodes]




if __name__ == '__main__':
    test_daq_build()
    test_daq_store()