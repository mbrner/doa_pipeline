import sqlalchemy as sa

from doa_pipeline.doa_pipeline import DOADataLayer, DOANodeConfig, Paused


def test_doa_dag_build(uri='sqlite:///:memory:'):
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
        node_b >> node_d
        node_c >> node_d

    
    with doa_datalayer(uri):
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


def test_await_events(uri='sqlite:///:memory:'):
    doa_datalayer = DOADataLayer('TestPause')

    config_node_1 = DOANodeConfig(name='1', version='0.0.0')
    config_node_2 = DOANodeConfig(name='2', version='0.0.0')
    config_node_3 = DOANodeConfig(name='3', version='0.0.0')
    config_node_4 = DOANodeConfig(name='4', version='0.0.0')
    with doa_datalayer.dag:
        node_1 = doa_datalayer.create_node(config_node_1)
        node_2 = doa_datalayer.create_node(config_node_2)
        node_3 = doa_datalayer.create_node(config_node_3)
        node_4 = doa_datalayer.create_node(config_node_4)
        node_1 >> node_2
        node_2 >> node_3
        node_3 >> node_4

    def check_status(doa_datalayer, id_, expected_status, expected_node_status, expected_awaited_events):
        table = doa_datalayer._table
        session = doa_datalayer._active_session
        q = session.query(table.c.status, table.c.node_status, table.c.awaited_events).filter(table.c.id == id_)
        res = session.execute(q)
        status, node_status, events = res.first()
        assert status == expected_status
        assert node_status == expected_node_status
        assert events == expected_awaited_events



    with doa_datalayer(uri):
        process_id = doa_datalayer.add_process()
        check_status(doa_datalayer, process_id, 'W','SWWWW', '')
        cfgs = [config_node_1, config_node_2, config_node_3, config_node_3, config_node_4]

        event = 'ein_wunder'
        queries = 0
        process_cxt = doa_datalayer.query_for_work(cfgs)
        with doa_datalayer.process(process_cxt) as result_container:
            check_status(doa_datalayer, process_id, 'R','SRWWW', '')
        check_status(doa_datalayer, process_id, 'W','SSWWW', '')

        process_cxt = doa_datalayer.query_for_work(cfgs)
        with doa_datalayer.process(process_cxt) as result_container:
            check_status(doa_datalayer, process_id, 'R','SSRWW', '')
            raise Paused(event, False)
        check_status(doa_datalayer, process_id, 'P','SSSWW', f'<{event}>')
        process_cxt = doa_datalayer.query_for_work(cfgs)
        assert process_cxt is None
        doa_datalayer.call_out_event(event)
        check_status(doa_datalayer, process_id, 'W','SSSWW', '')
        process_cxt = doa_datalayer.query_for_work(cfgs)
        with doa_datalayer.process(process_cxt) as result_container:
            check_status(doa_datalayer, process_id, 'R','SSSRW', '')
            raise Paused(event, True)
        check_status(doa_datalayer, process_id, 'P','SSSWW', f'<{event}>')
        doa_datalayer.resume()
        check_status(doa_datalayer, process_id, 'P','SSSWW', f'<{event}>')
        doa_datalayer.resume(force_resume=True)
        check_status(doa_datalayer, process_id, 'W','SSSWW', '')

            # if queries == 7:
            #     
            # print(f'{queries}: {process_cxt}')
            # if process_cxt is not None:
            #     print('Work')
            #     if process_cxt.config.name == config_node_2.name:
            #         with doa_datalayer.process(process_cxt) as result_container:
            #             raise Paused(event, False)
            #     else:
            #         with doa_datalayer.process(process_cxt) as result_container:
            #             pass
            


if __name__ == '__main__':
    #test_doa_dag_build('sqlite:///test.sqlite')
    test_await_events('sqlite:///test.sqlite')