from unittest                                                       import TestCase
from osbot_utils.utils.Http                                         import GET
from osbot_utils.utils.Env                                          import load_dotenv, get_env
from osbot_prefect.server.Prefect__Rest_API import Prefect__Rest_API, ENV_NAME__PREFECT_CLOUD__API_KEY, \
    ENV_NAME__PREFECT_TARGET_SERVER, DEFAULT_URL__PREFECT_TARGET_SERVER


class test_Prefect__Rest_API(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        load_dotenv()
        cls.prefect_rest_api = Prefect__Rest_API()

    def test_api_key(self):
        assert self.prefect_rest_api.prefect_cloud__api_key() == get_env(ENV_NAME__PREFECT_CLOUD__API_KEY)

    def test_prefect_api_url(self):
        expected_url = get_env(ENV_NAME__PREFECT_TARGET_SERVER, DEFAULT_URL__PREFECT_TARGET_SERVER)
        assert self.prefect_rest_api.prefect_api_url() == expected_url

    def test_local_server__api_status(self):
        if get_env(ENV_NAME__PREFECT_TARGET_SERVER):
            url_health = get_env(ENV_NAME__PREFECT_TARGET_SERVER) + '/health'
            assert url_health == 'http://localhost:4200/api/health'
            assert GET(url_health) == 'true'
