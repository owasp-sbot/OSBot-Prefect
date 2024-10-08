from unittest                                       import TestCase

from osbot_prefect.server.Prefect__Artifacts import Prefect__Artifacts
from osbot_utils.utils.Dev import pprint

from osbot_prefect.server.Prefect__States           import Prefect__States
from osbot_prefect.testing.Temp__Task_Run           import Temp__Task_Run
from osbot_prefect.testing.Temp__Flow_Run           import Temp__Flow_Run
from osbot_utils.utils.Misc                         import random_id, is_guid, random_text
from osbot_utils.utils.Env                          import load_dotenv
from osbot_prefect.server.Prefect__Cloud_API        import Prefect__Cloud_API
from osbot_utils.utils.Objects import dict_to_obj, obj_to_dict


class test_Prefect__Cloud_API(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        load_dotenv()
        cls.prefect_cloud_api = Prefect__Cloud_API()
        cls.flow_id           = cls.prefect_cloud_api.flow__create({'name': random_text('pytest-class-flow')}).data.id

    @classmethod
    def tearDownClass(cls):
        assert cls.prefect_cloud_api.flow__delete(cls.flow_id).status == "ok"

    def test__setUpClass(self):
        assert is_guid(self.flow_id)

    # valid artifacts types:

    def test_artifacts__create(self):
        with Temp__Task_Run() as _:
            artifacts_data = {"key": "an-key",
                              "type": Prefect__Artifacts.RESULT,
                              "description": "# an_description\n\n- an item",
                              "data": {"answer": "# 42 \n\n- an item"},
                              "metadata_": {
                                "additional_prop_1": "an_prop_1",
                                "additional_prop_2": "an_prop_2",
                                "additional_prop_3": "an_prop_2"
                              },
                              "flow_run_id": _.flow_run_id,
                              "task_run_id": _.task_run_id
                            }
            #pprint(_.prefect_cloud_api.artifacts__create(artifacts_data))
            _.flow_run__set_state__running()
            _.task_run__set_state__running()
            assert _.prefect_cloud_api.artifacts__create(artifacts_data).status == 'ok'
            _.task_run__set_state__completed()
            _.flow_run__set_state__completed()
            print()

    def test_flow_create(self):
        with self.prefect_cloud_api as _:
            flow_definition = { "name": random_id(prefix="pytest-method-flow"),
                                "tags": [ "created-by-pytest"   ,
                                          "local-prefect-server"]}

            flow_data_1       = _.flow__create(flow_definition).data
            flow_data_2       = _.flow__create(flow_definition).data
            flow_id           = flow_data_1.id
            flow_name         = flow_data_1.name
            flow_tags         = flow_data_1.tags
            flow_data_3       = _.flow(flow_id).data

            delete_response_1 = _.flow__delete(flow_id)
            delete_response_2 = _.flow__delete(flow_id)

            assert is_guid(flow_id) is True
            assert flow_name        ==  flow_definition.get('name')
            assert flow_tags        == flow_definition.get('tags')
            assert flow_data_1      == flow_data_2
            assert flow_data_1      == flow_data_3

            assert delete_response_1.status == "ok"
            assert delete_response_2.status == "error"


    def test_flow_run__create__update(self):
        flow_name           = random_id(prefix="flow-name")
        tags_1              = ["aaa", "bbb", "ccc"]
        tags_2              = ["ddd", "eee", "fff"]
        flow_run_definition = { "name"   : flow_name             ,
                                "flow_id": self.flow_id           ,
                                "state"  : { "type": "SCHEDULED" },
                                "tags"   : tags_1                }
        flow_run_update     = { 'tags'   : tags_2                }

        with self.prefect_cloud_api as _:
            flow_run_1 = _.flow_run__create(flow_run_definition).data
            flow_run_id = flow_run_1.id
            flow_run_2  = _.flow_run(flow_run_id).data

            assert dict_to_obj(flow_run_1).flow_id == self.flow_id
            assert is_guid(flow_run_id) is True
            assert flow_run_1.name                            == flow_name
            assert flow_run_1.tags                            == tags_1
            assert flow_run_1.flow_id                         == self.flow_id
            assert flow_run_1.run_count                       == 0
            assert flow_run_1.state.type                      == 'SCHEDULED'
            assert flow_run_1.state.name                      == 'Scheduled'
            assert flow_run_1.parent_task_run_id              is None
            assert flow_run_1.state.state_details.flow_run_id == flow_run_id
            assert flow_run_1.empirical_policy.max_retries    == 0

            delattr(flow_run_1, 'estimated_start_time_delta')
            delattr(flow_run_2, 'estimated_start_time_delta')

            assert flow_run_1 == flow_run_2

            assert _.flow_run__update(flow_run_1.id, flow_run_update).status == 'ok'

            flow_run_3 = _.flow_run(flow_run_id).data
            assert flow_run_3.tags                        == tags_2
            assert _.flow_run__delete(flow_run_id).status == 'ok'

    def test_flow_run__input(self):
        with Temp__Flow_Run() as _:
            input_data = { "key"   : "string" ,
                           "value" : "string" ,
                           "sender": "string" }
            assert _.prefect_cloud_api.flow_run__input(_.flow_run_id, input_data).status == 'ok'
            # todo: add a test that checks that the input data is correctly set and how to use it

    def test_flow_run__set_state(self):
        with Temp__Flow_Run() as temp_flow_run:                                                 # todo: refactor code below to make better use of the Temp__Flow_Run class
            _        = temp_flow_run.prefect_cloud_api
            flow_run = temp_flow_run.flow_run
            assert _.flow_run(flow_run.id).data.state.type == "PENDING"
            for value in Prefect__States().__locals__().values():                               # todo: see if need to test all these
                state_data = { "type": value }
                assert _.flow_run__set_state(flow_run.id, state_data ).status == 'ok'
                if value not in ['COMPLETED', 'FAILED', 'CRASHED', 'PAUSED']:                   # after CANCELLED none of these work
                    assert _.flow_run(flow_run.id).data.state.type == value
            assert _.flow_run__delete(flow_run.id).status == 'ok'

    def test_flows(self):
        flows    = self.prefect_cloud_api.flows()
        for flow in flows:
            assert is_guid(flow.id)

    def test_flow(self):
        with self.prefect_cloud_api as _:
            flows_ids = _.flows_ids()
            if flows_ids:
                flow_id   = flows_ids.pop()
                flow      = self.prefect_cloud_api.flow(flow_id=flow_id).data
                assert flow.id == flow_id

    def test_logs_create(self):
        with Temp__Task_Run() as _:
            log_item_0 = dict_to_obj({"name"       : "log_to_flow", "level": 0 , "message": "log_to_flow"                        ,
                                      "timestamp"  : self.prefect_cloud_api.to_prefect_timestamp__now_utc()                      ,
                                      "flow_run_id": _.flow_run_id                                                               })
            log_item_1 = dict_to_obj({"name"       : "log_to_task", "level": 10, "message": "log_to_task"                        ,
                                      "timestamp"  : self.prefect_cloud_api.to_prefect_timestamp__now_utc__with_delta(seconds=-5) ,
                                      "task_run_id": _.task_run_id                                                               })
            log_item_2 = dict_to_obj({"name"       : "log_to_both", "level": 20, "message": "log_to_both"                        ,
                                      "timestamp"  : self.prefect_cloud_api.to_prefect_timestamp__now_utc__with_delta(seconds=-10),
                                      "task_run_id": _.task_run_id, "flow_run_id": _.flow_run_id                                 })

            _.flow_run__set_state__running()
            _.task_run__set_state__running()

            assert _.prefect_cloud_api.logs__create([obj_to_dict(log_item_0)]).status == 'ok'
            assert _.prefect_cloud_api.logs__create([obj_to_dict(log_item_1)]).status == 'ok'
            assert _.prefect_cloud_api.logs__create([obj_to_dict(log_item_2)]).status == 'ok'

            logs = _.prefect_cloud_api.logs__filter().data
            assert logs[0].name        == log_item_0.name
            assert logs[0].level       == log_item_0.level
            assert logs[0].message     == log_item_0.message
            assert logs[0].timestamp   == log_item_0.timestamp
            assert logs[0].flow_run_id == log_item_0.flow_run_id
            assert logs[0].task_run_id is None

            # todo: figure out why the checks below fail intermittently (also add the test for log_item_2)
            # assert logs[1].name        == log_item_1.name
            # assert logs[1].level       == log_item_1.level
            # assert logs[1].message     == log_item_1.message
            # assert logs[1].timestamp   == log_item_1.timestamp
            # assert logs[1].flow_run_id is None
            # assert logs[1].task_run_id == log_item_1.task_run_id





    def test_task_run__create(self):
        with Temp__Task_Run() as _:
            assert _.flow_run__info().state.type == "PENDING"
            assert _.flow_run__set_state(Prefect__States.RUNNING).data.status == 'ACCEPT'
            assert _.flow_run__info().state.type == "RUNNING"
            assert _.task_run__info().state.type == "PENDING"
            assert _.task_run__set_state(Prefect__States.RUNNING).data.status == 'ACCEPT'
            assert _.task_run__info().state.type == "RUNNING"
            assert _.task_run__set_state(Prefect__States.COMPLETED).data.status == 'ACCEPT'
            assert _.task_run__info().state.type == "COMPLETED"