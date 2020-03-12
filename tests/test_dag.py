import pytest

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
    print(sorted_node_names)
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


if __name__ == '__main__':
    test_daq_build()