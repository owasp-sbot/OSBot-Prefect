from types import SimpleNamespace

from osbot_utils.utils.Dev import pprint

from osbot_utils.helpers.Random_Guid import Random_Guid

from osbot_prefect.server.Prefect__Cloud_API import Prefect__Cloud_API
from osbot_utils.utils.Misc import random_text

from osbot_utils.base_classes.Type_Safe import Type_Safe


class Temp__Flow(Type_Safe):
    flow_name         : str             = random_text('pytest-temp-flow')
    flow              : SimpleNamespace = None
    flow_id           : str             = None
    prefect_cloud_api: Prefect__Cloud_API

    def __enter__(self):
        self.flow__create()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flow__delete()

    def flow__create(self):
        self.flow    = self.prefect_cloud_api.flow__create({'name': self.flow_name}).data
        self.flow_id = self.flow.id
        return self

    def flow__delete(self):
        self.prefect_cloud_api.flow__delete(self.flow_id)

    def flow__exists(self):
        return self.flow__info() is not None

    def flow__info(self):
        return self.prefect_cloud_api.flow(self.flow_id).data

