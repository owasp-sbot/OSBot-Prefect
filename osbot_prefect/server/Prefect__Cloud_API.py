from datetime                                   import datetime, timezone, timedelta
from osbot_prefect.server.Prefect__States       import Prefect__States
from osbot_utils.base_classes.Type_Safe         import Type_Safe
from osbot_prefect.server.Prefect__Rest_API     import Prefect__Rest_API

class Prefect__Cloud_API(Type_Safe):
    prefect_rest_api = Prefect__Rest_API()

    def artifacts__create(self, artifact_data):
        return self.prefect_rest_api.create(target='artifacts', data=artifact_data)

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

    def flow_run__set_state_type(self, flow_run_id, state_type):
        return self.flow_run__set_state(flow_run_id, {'type': state_type})

    def flow_run__set_state_type__running(self, flow_run_id):
        return self.flow_run__set_state_type(flow_run_id, Prefect__States.RUNNING)

    def flow_run__set_state_type__completed(self, flow_run_id):
        return self.flow_run__set_state_type(flow_run_id, Prefect__States.COMPLETED)

    def flow_run__delete(self, flow_run_id):
        return self.prefect_rest_api.delete(target='flow_runs', target_id=flow_run_id)

    def flow_run__update(self, flow_run_id, flow_run_definition):
        return self.prefect_rest_api.update(target='flow_runs', target_id=flow_run_id, target_data=flow_run_definition)

    def flows(self, limit=5):
        filter_data = {"sort": "CREATED_DESC",
                       "limit": limit}
        return self.prefect_rest_api.filter(target='flows', filter_data=filter_data).data or []

    def flows_ids(self, limit=5):                                       # todo: see if there is a way to get these IDs directly via a GraphQL query
        flows = self.flows(limit=limit)
        return [flow.id for flow in flows]

    def logs__create(self, log_data):
        return self.prefect_rest_api.create(target='logs', data=log_data)

    def logs__filter(self, limit=5):
        filter_data = {"sort": "TIMESTAMP_DESC",
                       "limit": limit}
        return self.prefect_rest_api.filter(target='logs', filter_data=filter_data)

    def task_run(self, task_run_id):
        return self.prefect_rest_api.read(target='task_runs', target_id=task_run_id)


    def task_run__create(self, task_run_definition):
        return self.prefect_rest_api.create(target='task_runs', data=task_run_definition)

    def task_run__set_state(self, task_run_id, state):
        kwargs = dict(target        = 'task_runs'       ,
                      target_id     = task_run_id       ,
                      target_action = 'set_state'       ,
                      target_data   = { 'state': state })

        return self.prefect_rest_api.update_action(**kwargs)

    def task_run__set_state_type(self, task_run_id, state_type):
        return self.task_run__set_state(task_run_id, {'type': state_type})

    def task_run__set_state_type__running(self, task_run_id):
        return self.task_run__set_state_type(task_run_id, Prefect__States.RUNNING)

    def task_run__set_state_type__running__completed(self, task_run_id):
        return self.task_run__set_state_type(task_run_id, Prefect__States.COMPLETED)

    def to_prefect_timestamp(self, date_time):
        return date_time.isoformat()
        #return date_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def to_prefect_timestamp__now_utc(self):
        return self.to_prefect_timestamp(datetime.now(timezone.utc))

    def to_prefect_timestamp__now_utc__with_delta(self, hours=0, minutes=0, seconds=0, milliseconds=0,microseconds=0):
        current_time = datetime.now(timezone.utc)
        new_time     = current_time + timedelta(hours=hours,minutes=minutes, seconds=seconds,milliseconds=milliseconds, microseconds=microseconds)
        return self.to_prefect_timestamp(new_time)

