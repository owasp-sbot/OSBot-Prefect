from types import SimpleNamespace

from osbot_prefect.server.Prefect__States import Prefect__States
from osbot_prefect.testing.Temp__Flow import Temp__Flow


class Temp__Flow_Run(Temp__Flow):
    flow_run_id: str             = None
    flow_run   : SimpleNamespace = None

    def __enter__(self):
        super().__enter__()
        self.flow_run__create()
        return self

    def flow_run__create(self):
        self.flow_run    = self.prefect_cloud_api.flow_run__create({'flow_id': self.flow_id}).data
        self.flow_run_id = self.flow_run.id
        return self

    def flow_run__exists(self):
        return self.flow_run__info() is not None

    def flow_run__info(self):
        return self.prefect_cloud_api.flow_run(self.flow_run_id).data

    def flow_run__set_state(self, state):
        return self.prefect_cloud_api.flow_run__set_state_type(self.flow_run_id, state)

    def flow_run__set_state__completed(self):
        return self.flow_run__set_state(Prefect__States.COMPLETED)

    def flow_run__set_state__running(self):
        return self.flow_run__set_state(Prefect__States.RUNNING)