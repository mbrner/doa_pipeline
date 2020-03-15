import pytest
import sqlalchemy as sa


from doa_pipeline.doa_pipeline import DOADataLayer, DOANodeConfig
from doa_pipeline.dag import DAG, Node, Edge, DETACHED


def test_doa_dag_build():
    doa_datalayer = DOADataLayer('Test')

    config_node_a = DOANodeConfig(name='a', version='0.0.0', result_columns=[sa.Column('test_col', sa.Text, default='')])
    config_node_b = DOANodeConfig(name='b', version='0.0.0')
    config_node_c = DOANodeConfig(name='c', version='0.0.0')
    with doa_datalayer.dag:
        node_a = doa_datalayer.create_node(config_node_a)
        node_b = doa_datalayer.create_node(config_node_b)
        node_c = doa_datalayer.create_node(config_node_c)
        node_a << node_c
        node_b >> node_a
    with doa_datalayer(r'sqlite://'):
        doa_datalayer.add_process()
        doa_datalayer.query_for_work(config_node_b)
        doa_datalayer.query_for_work(config_node_a)


if __name__ == '__main__':
    test_doa_dag_build()