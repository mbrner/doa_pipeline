import pytest

from doa_pipeline.doa_pipeline import DOADataLayer, DOANodeConfig
from doa_pipeline.dag import DAG, Node, Edge, DETACHED


def test_doa_dag_build():
    doa_datalayer = DOADataLayer('Test')

    config_node_a = DOANodeConfig(name='a', version='0.0.0')
    config_node_b = DOANodeConfig(name='b', version='0.0.0')
    config_node_c = DOANodeConfig(name='c', version='0.0.0')
    with doa_datalayer.dag:
        node_a = doa_datalayer.create_node(config_node_a)
        node_b = doa_datalayer.create_node(config_node_b)
        node_c = doa_datalayer.create_node(config_node_c)
        print(node_a)
        node_a << node_c
        node_b >> node_a

    table = doa_datalayer.build_db_table()


if __name__ == '__main__':
    test_doa_dag_build()