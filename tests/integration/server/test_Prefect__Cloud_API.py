from unittest                                       import TestCase

from osbot_utils.utils.Dev import pprint

from osbot_utils.utils.Misc                         import list_set, random_id, is_guid, random_text
from osbot_utils.utils.Env                          import load_dotenv, get_env
from osbot_prefect.server.Prefect__Cloud_API        import Prefect__Cloud_API
from osbot_utils.utils.Objects                      import dict_to_obj, obj_data, obj_to_dict


class test_Prefect__Cloud_API(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        load_dotenv()
        cls.prefect_cloud_api = Prefect__Cloud_API()
        cls.flow_id           = cls.prefect_cloud_api.flow__create({'name': random_text('pytest-class-flow')}).id

    @classmethod
    def tearDownClass(cls):
        assert cls.prefect_cloud_api.flow__delete(cls.flow_id) is True

    def test__setUpClass(self):
        assert is_guid(self.flow_id)

    def test_flow_create(self):
        with self.prefect_cloud_api as _:
            flow_definition = { "name": random_id(prefix="pytest-method-flow"),
                                "tags": [ "created-by-pytest"   ,
                                          "local-prefect-server"]}

            flow_data_1       = _.flow__create(flow_definition)
            flow_data_2       = _.flow__create(flow_definition)
            flow_id           = flow_data_1.id
            flow_name         = flow_data_1.name
            flow_tags         = flow_data_1.tags
            flow_data_3       = _.flow(flow_id)

            delete_response_1 = _.flow__delete(flow_id)
            delete_response_2 = _.flow__delete(flow_id)

            assert is_guid(flow_id) is True
            assert flow_name        ==  flow_definition.get('name')
            assert flow_tags        == flow_definition.get('tags')
            assert flow_data_1      == flow_data_2
            assert flow_data_1      == flow_data_3

            assert delete_response_1 is True
            assert delete_response_2 is False


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
            flow_run_1 = _.flow_run__create(flow_run_definition)
            flow_run_id = flow_run_1.id
            flow_run_2  = _.flow_run(flow_run_id)

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

            assert _.flow_run__update(flow_run_1.id, flow_run_update) is True

            flow_run_3 = _.flow_run(flow_run_id)
            assert flow_run_3.tags                 == tags_2
            assert _.flow_run__delete(flow_run_id) is True

    def test_flow_run__set_state(self):
        flow_run_state_1    = { "type": "RUNNING"   }
        #flow_run_state_2    = { "type": "COMPLETED" }

        flow_name           = random_id(prefix="flow-name")
        flow_run_definition = { "name": flow_name,
                                "flow_id": self.flow_id}
        with self.prefect_cloud_api as _:
            flow_run = _.flow_run__create(flow_run_definition)

            assert _.flow_run(flow_run.id).state.type == "PENDING"
            assert _.flow_run__set_state(flow_run.id, flow_run_state_1) is True
            assert _.flow_run(flow_run.id).state.type == "RUNNING"
            assert _.flow_run__delete(flow_run.id) is True

    def test_flows(self):
        flows    = self.prefect_cloud_api.flows()
        for flow in flows:
            assert is_guid(flow.id)

    def test_flow(self):
        with self.prefect_cloud_api as _:
            flows_ids = _.flows_ids()
            if flows_ids:
                flow_id   = flows_ids.pop()
                flow      = self.prefect_cloud_api.flow(flow_id=flow_id)
                assert flow.id == flow_id