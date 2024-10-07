from enum import Enum

from osbot_utils.utils.Dev import pprint

from osbot_utils.base_classes.Type_Safe         import Type_Safe
from osbot_prefect.server.Prefect__Rest_API     import Prefect__Rest_API
from osbot_utils.utils.Objects import dict_to_obj


class Prefect__States(Type_Safe):
    SCHEDULED : str = "SCHEDULED"
    PENDING   : str = "PENDING"
    RUNNING   : str = "RUNNING"
    CANCELLING: str = "CANCELLING"
    CANCELLED : str = "CANCELLED"
    COMPLETED : str = "COMPLETED"
    FAILED    : str = "FAILED"
    CRASHED   : str = "CRASHED"
    PAUSED    : str = "PAUSED"

class Prefect__Cloud_API(Type_Safe):
    prefect_rest_api = Prefect__Rest_API()

    def flow(self, flow_id):
        return self.prefect_rest_api.read(target='flows', target_id=flow_id)

    def flow__create(self, flow_definition):
        return self.prefect_rest_api.create(target='flows', data=flow_definition)

    def flow__delete(self, flow_id):
        return  self.prefect_rest_api.delete(target='flows', target_id=flow_id)

    def flow_run(self, flow_id):
        return self.prefect_rest_api.read(target='flow_runs', target_id=flow_id)

    def flow_run__create(self, flow_run_definition):
        return self.prefect_rest_api.create(target='flow_runs', data=flow_run_definition)

    def flow_run__input(self, flow_run_id, input_data):
        kwargs = dict(target        = 'flow_runs'       ,
                      target_id     = flow_run_id       ,
                      target_action = 'input'           ,
                      target_data   = input_data        )

        return self.prefect_rest_api.update_action(**kwargs)

    def flow_run__set_state(self, flow_run_id, state):
        kwargs = dict(target        = 'flow_runs'       ,
                      target_id     = flow_run_id       ,
                      target_action = 'set_state'       ,
                      target_data   = { 'state': state })

        return self.prefect_rest_api.update_action(**kwargs)

    def flow_run__delete(self, flow_run_id):
        return self.prefect_rest_api.delete(target='flow_runs', target_id=flow_run_id)

    def flow_run__update(self, flow_run_id, flow_run_definition):
        return self.prefect_rest_api.update(target='flow_runs', target_id=flow_run_id, target_data=flow_run_definition)

    def flows(self, limit=5):
        return self.prefect_rest_api.filter(target='flows', limit=limit).data or []

    def flows_ids(self, limit=5):                                       # todo: see if there is a way to get these IDs directly via a GraphQL query
        flows = self.flows(limit=limit)
        return [flow.id for flow in flows]

    def task_run__create(self, task_run_definition):
        return self.prefect_rest_api.create(target='task_runs', data=task_run_definition)



