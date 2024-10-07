from unittest import TestCase

from osbot_utils.utils.Dev import pprint

from osbot_utils.utils.Misc import is_guid

from osbot_prefect.testing.Temp__Flow_Run import Temp__Flow_Run
from osbot_utils.utils.Env import load_dotenv


class test_Temp__Flow_Run(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        load_dotenv()

    def test__enter_exit(self):
        temp_flow_run = Temp__Flow_Run()
        assert temp_flow_run.flow        is None
        assert temp_flow_run.flow_id     is None
        assert temp_flow_run.flow_run_id is None

        with temp_flow_run as _:
            assert type(_) is Temp__Flow_Run
            assert is_guid(_.flow_id    ) is True
            assert is_guid(_.flow_run_id) is True
            assert _.flow_run.flow_id     == _.flow_id
            assert _.flow_run.state.type  == "PENDING"
            assert _.flow_run__info().id  == _.flow_run_id
            assert _.flow_run__exists()   is True

        assert _.flow_run__exists() is False
        assert _.flow__exists    () is False
