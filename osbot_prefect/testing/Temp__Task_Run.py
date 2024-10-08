from types                                  import SimpleNamespace
from osbot_prefect.server.Prefect__States   import Prefect__States
from osbot_utils.utils.Misc                 import random_text
from osbot_utils.helpers.Random_Guid        import Random_Guid
from osbot_prefect.testing.Temp__Flow_Run   import Temp__Flow_Run


class Temp__Task_Run(Temp__Flow_Run):
    task_run_definition     : dict
    task_run_dynamic_key    : str              = random_text('task-run-dynamic-key')
    task_run                : SimpleNamespace  = None
    task_run_key            : Random_Guid
    task_run_id             : Random_Guid      = None
    task_run__create_result : SimpleNamespace  = None

    def __enter__(self):
        super().__enter__()
        self.task_run__create()
        return self

    def task_run__create(self, **task_run_definition):
        self.task_run_definition = { 'flow_run_id': self.flow_run_id         ,
                                     'dynamic_key': self.task_run_dynamic_key,
                                     'task_key'   : self.task_run_key        ,
                                     **task_run_definition   }
        self.task_run__create_result = self.prefect_cloud_api.task_run__create(self.task_run_definition)
        self.task_run                = self.task_run__create_result.data
        self.task_run_id             = Random_Guid(self.task_run.id)
        return self

    def task_run__exists(self):
        return self.task_run__info() is not None

    def task_run__info(self):
        return self.prefect_cloud_api.task_run(self.task_run_id).data

    def task_run__set_state(self, state):
        return self.prefect_cloud_api.task_run__set_state_type(self.task_run_id, state)

    def task_run__set_state__completed(self):
        return self.task_run__set_state(Prefect__States.COMPLETED)

    def task_run__set_state__running(self):
        return self.task_run__set_state(Prefect__States.RUNNING)