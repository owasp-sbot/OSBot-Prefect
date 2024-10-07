from unittest import TestCase

from osbot_utils.utils.Misc import is_guid

from osbot_utils.utils.Env              import load_dotenv
from osbot_utils.utils.Dev              import pprint
from osbot_prefect.testing.Temp__Flow   import Temp__Flow


class test_Temp__Flow(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        load_dotenv()

    def test__enter_exit(self):

        temp_flow = Temp__Flow()
        assert temp_flow.flow    is None
        assert temp_flow.flow_id is None

        with temp_flow as _:
            assert type(_) is Temp__Flow
            assert is_guid(_.flow_id) is True
            assert str(type(_.flow))  == "<class 'osbot_utils.utils.Objects._'>"
            assert _.flow__info()           == _.flow
            assert _.flow__exists()         is True
        assert _.flow__info  () is None
        assert _.flow__exists() is False