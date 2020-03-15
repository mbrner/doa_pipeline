import pytest
import sqlalchemy as sa


from doa_pipeline.doa_pipeline import DOADataLayer, DOANodeConfig
from doa_pipeline.dag import DAG, Node, Edge, DETACHED


def test_doa_dag_build():
    doa_datalayer = DOADataLayer('Test')

    config_node_a = DOANodeConfig(name='a', version='0.0.0', result_columns=[sa.Column('test_col', sa.Text, default='')])
    config_node_b = DOANodeConfig(name='b', version='0.0.0')
    config_node_c = DOANodeConfig(name='c', version='0.0.0')
    config_node_d = DOANodeConfig(name='d', version='0.0.0')
    with doa_datalayer.dag:
        node_a = doa_datalayer.create_node(config_node_a)
        node_b = doa_datalayer.create_node(config_node_b)
        node_c = doa_datalayer.create_node(config_node_c)
        node_d = doa_datalayer.create_node(config_node_d)
        node_a << node_c
        node_b >> node_a
        node_a >> node_d
    with doa_datalayer(r'sqlite://'):#/test.sqlite'):
        doa_datalayer.add_process()
        process_cxt = doa_datalayer.query_for_work(config_node_c)
        with doa_datalayer.process(process_cxt) as result_container:
            pass
        process_cxt = doa_datalayer.query_for_work(config_node_b)
        with doa_datalayer.process(process_cxt) as result_container:
            pass
        process_cxt = doa_datalayer.query_for_work(config_node_a)
        with doa_datalayer.process(process_cxt) as result_container:
            result_container.test_col = 'Das ist ein Test'
        process_cxt = doa_datalayer.query_for_work(config_node_d)
        with doa_datalayer.process(process_cxt) as result_container:
            raise ValueError('JAJAJA')


if __name__ == '__main__':
    test_doa_dag_build()