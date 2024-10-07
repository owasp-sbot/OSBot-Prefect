from types import SimpleNamespace

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