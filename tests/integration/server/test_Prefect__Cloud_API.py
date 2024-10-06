from unittest                                                           import TestCase

from osbot_utils.utils.Dev import pprint
from osbot_utils.utils.Lists                                            import list_in_list
from osbot_utils.utils.Misc import list_set, random_id, is_guid, random_text
from osbot_utils.utils.Env                                              import load_dotenv, get_env

from osbot_prefect.server.Prefect__Cloud_API import Prefect__Cloud_API
from osbot_prefect.server.Prefect__Rest_API import ENV_NAME__PREFECT_TARGET_SERVER


class test_Prefect__Cloud_API(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        load_dotenv()
        cls.prefect_cloud_api = Prefect__Cloud_API()
        cls.flow_id           = cls.prefect_cloud_api.flow__create({'name': random_text('pytest-class-flow')}).get('id')

    @classmethod
    def tearDownClass(cls):
        assert cls.prefect_cloud_api.flow__delete(cls.flow_id) is True

    def test__setUpClass(self):
        assert is_guid(self.flow_id)


    def test_confirm_local_docker(self):
        assert get_env(ENV_NAME__PREFECT_TARGET_SERVER) == 'http://localhost:4200/api'

    def test_flow_create(self):
        with self.prefect_cloud_api as _:
            flow_definition = { "name": random_id(prefix="pytest-method-flow"),
                                "tags": [ "created-by-pytest"   ,
                                          "local-prefect-server"]}
            flow_data_1       = _.flow__create(flow_definition)
            flow_data_2       = _.flow__create(flow_definition)
            flow_id           = flow_data_1.get('id'  )
            flow_name         = flow_data_1.get('name')
            flow_tags         = flow_data_1.get('tags')
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


    def test_flow_run__create(self):
        flow_run_definition = { "name": "my-flow-run",
                                "flow_id": self.flow_id }
        #pprint(flow_run_definition)

    def test_flows(self):
        flows    = self.prefect_cloud_api.flows()
        for flow in flows:
            assert list_in_list(["created", "id", "tags", "updated"], list_set(flow)) is True

    def test_flow(self):
        with self.prefect_cloud_api as _:
            flows_ids = _.flows_ids()
            if flows_ids:
                flow_id   = flows_ids.pop()
                flow      = self.prefect_cloud_api.flow(flow_id=flow_id)
                assert list_in_list(["created", "id", "tags", "updated"], list_set(flow)) is True