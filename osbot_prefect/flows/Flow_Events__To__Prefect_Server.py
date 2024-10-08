from osbot_prefect.utils.for__osbot_aws import in_aws_lambda
from osbot_utils.utils.Env import in_github_action

from osbot_utils.utils.Misc import time_now

from osbot_utils.helpers.Random_Guid import Random_Guid

from osbot_utils.helpers.flows.Task import Task

from osbot_prefect.server.Prefect__States import Prefect__States
from osbot_utils.utils.Dev import pprint

from osbot_prefect.server.Prefect__Cloud_API    import Prefect__Cloud_API
from osbot_utils.helpers.flows.Flow             import Flow
from osbot_utils.base_classes.Type_Safe         import Type_Safe
from osbot_utils.helpers.flows.Flow__Events     import flow_events, Flow__Event_Type, Flow__Event


class Flow_Events__To__Prefect_Server(Type_Safe):
    prefect_cloud_api   : Prefect__Cloud_API
    prefect_ids_mapping : dict

    def __enter__(self):
        self.add_event_listener()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_event_listener()

    def add_event_listener(self):
        flow_events.event_listeners.append(self.event_listener)

    def handle_event(self, event_type: Flow__Event_Type, event_source, event_data):
        if   event_type == Flow__Event_Type.FLOW_START:  self.handle_event__flow_start(flow=event_source)
        elif event_type == Flow__Event_Type.FLOW_STOP :  self.handle_event__flow_stop (flow=event_source)
        elif event_type == Flow__Event_Type.TASK_START:  self.handle_event__task_start(task=event_source)
        elif event_type == Flow__Event_Type.TASK_STOP :  self.handle_event__task_stop (task=event_source)
        else:
            print()
            print(f"Error in handle_event, unknown event_type: {event_type}")

    def current_execution_environment(self):
        if in_github_action():
            return  'github_action'
        elif in_aws_lambda():
            return 'aws_lambda'
        else:
            return 'local'

    def handle_event__flow_start(self, flow: Flow):
        prefect__flow_id                         = self.prefect_cloud_api.flow__create({'name': flow.flow_name}).data.id
        tag__current_env                         = self.current_execution_environment()
        tag__current_time                        = time_now()
        prefect__flow_run_definition             = dict(flow_id    = prefect__flow_id                            ,
                                                        name       = flow.flow_id                                ,
                                                        parameters = dict(answer = 42                            ,
                                                                          source = 'handle_event__flow_start'   ),
                                                        context    = dict(context_1 = 42                         ,
                                                                          context_2 = 'handle_event__flow_start'),
                                                        tags       = [tag__current_env, tag__current_time       ])
        prefect_flow_run                         = self.prefect_cloud_api.flow_run__create(prefect__flow_run_definition)
        if prefect_flow_run.status != 'ok':
            pprint("******* Error in handle_event__flow_start ***** ")          # todo: move this to a Flow Events logging system
            pprint(prefect_flow_run)
        else:
            prefect__flow_run_id                     = prefect_flow_run.data.id
            self.prefect_ids_mapping[flow.flow_name] = prefect__flow_id
            self.prefect_ids_mapping[flow.flow_id  ] = prefect__flow_run_id
            self.prefect_cloud_api.flow_run__set_state_type__running(prefect__flow_run_id)

    def handle_event__flow_stop(self, flow: Flow):
        prefect__flow_run_id = self.prefect_ids_mapping.get(flow.flow_id)
        self.prefect_cloud_api.flow_run__set_state_type__completed(prefect__flow_run_id)

    def handle_event__task_start(self, task: Task):
        prefect__flow_run_id          = self.prefect_ids_mapping[task.task_flow.flow_id]
        prefect__task_run_definition  = { 'flow_run_id' : prefect__flow_run_id,
                                          'dynamic_key' : Random_Guid()       ,
                                          'task_key'    : Random_Guid()       ,
                                          'name'        : task.task_name      ,
                                          'task_inputs' : {"prop_1": [{"input_type": "parameter"    ,
                                                                       "name"      : "an-parameter" },
                                                                      {"input_type": "constant"     ,
                                                                        "type"     :"an-type"       }]},
                                          "tags"        : ["tag_a", "tag_b"] }
        prefect__task_run    = self.prefect_cloud_api.task_run__create(prefect__task_run_definition)
        if prefect__task_run.status != 'ok':
            pprint("******* Error in handle_event__task_start ***** ")          # todo: move this to a Flow Events logging system
            pprint(prefect__task_run)
        else:
            prefect__task_run_id = prefect__task_run.data.id
            self.prefect_ids_mapping[task.task_id] = prefect__task_run_id
            self.prefect_cloud_api.task_run__set_state_type__running(prefect__task_run_id)

    def handle_event__task_stop(self, task):
        prefect__task_run_id = self.prefect_ids_mapping.get(task.task_id)
        self.prefect_cloud_api.task_run__set_state_type__running__completed(prefect__task_run_id)

    def event_listener(self, flow_event: Flow__Event):
        event_type   = flow_event.event_type
        event_source = flow_event.event_source
        event_data   = flow_event.event_data
        self.handle_event(event_type=event_type, event_source=event_source, event_data=event_data)

    def remove_event_listener(self):
        flow_events.event_listeners.remove(self.event_listener)