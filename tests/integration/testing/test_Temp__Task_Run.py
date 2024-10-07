from unittest import TestCase

from osbot_utils.utils.Misc import is_guid

from osbot_utils.utils.Dev import pprint

from osbot_prefect.testing.Temp__Task_Run import Temp__Task_Run
from osbot_utils.utils.Env import load_dotenv
from osbot_utils.utils.Objects import dict_to_obj


class test_Temp__Task_Run(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        load_dotenv()

    def test__enter_exit(self):
        temp_task_run = Temp__Task_Run()
        assert temp_task_run.flow                  is None
        assert temp_task_run.flow_id               is None
        assert temp_task_run.flow_run_id           is None
        assert temp_task_run.task_run_id           is None
        assert is_guid(temp_task_run.task_run_key) is True
        assert temp_task_run.task_run_definition   == {}
        assert temp_task_run.task_run_dynamic_key.startswith('task-run-dynamic-key')

        with temp_task_run as _:
            assert type(_) is Temp__Task_Run
            assert _.task_run__create_result.status == 'ok'
            assert is_guid(_.task_run.task_key) is True
            assert _.task_run.id                == _.task_run_id
            assert _.task_run.task_key          == _.task_run_key
            assert _.task_run.flow_run_id       == _.flow_run_id
            assert _.task_run.dynamic_key       == _.task_run_dynamic_key
            assert _.task_run.state.message     is None
            assert _.task_run.state_type        == "PENDING"
            assert _.task_run.state.type        == "PENDING"
            assert _.task_run.run_count         == 0
            assert _.task_run__exists()         is True

            task_run_info_1 = _.task_run__info()
            task_run_info_2 = _.prefect_cloud_api.task_run(_.task_run_id).data
            delattr(task_run_info_1, 'estimated_start_time_delta')
            delattr(task_run_info_2, 'estimated_start_time_delta')
            assert task_run_info_1 == task_run_info_2

        assert _.task_run__exists() is False



