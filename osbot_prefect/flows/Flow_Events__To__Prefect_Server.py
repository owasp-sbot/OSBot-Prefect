from osbot_prefect.server.Prefect__Artifacts                import Prefect__Artifacts
from osbot_prefect.utils.for__osbot_aws                     import in_aws_lambda
from osbot_utils.helpers.flows.models.Flow_Run__Event_Data  import Flow_Run__Event_Data
from osbot_utils.utils.Env                                  import in_github_action
from osbot_utils.utils.Misc                                 import time_now, timestamp_to_datetime
from osbot_utils.helpers.Random_Guid                        import Random_Guid
from osbot_utils.utils.Dev                                  import pprint
from osbot_prefect.server.Prefect__Cloud_API                import Prefect__Cloud_API
from osbot_utils.base_classes.Type_Safe                     import Type_Safe
from osbot_utils.helpers.flows.Flow__Events                 import flow_events, Flow_Run__Event_Type, Flow_Run__Event


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

    def handle_event(self, flow_event: Flow_Run__Event):
        event_type   = flow_event.event_type
        event_data   = flow_event.event_data
        if   event_type == Flow_Run__Event_Type.FLOW_MESSAGE: self.handle_event__task_message(flow_event = flow_event)
        elif event_type == Flow_Run__Event_Type.FLOW_START  : self.handle_event__flow_start  (event_data = event_data)
        elif event_type == Flow_Run__Event_Type.FLOW_STOP   : self.handle_event__flow_stop   (event_data = event_data)
        elif event_type == Flow_Run__Event_Type.NEW_RESULT  : self.handle_event__new_result  (event_data = event_data)
        elif event_type == Flow_Run__Event_Type.NEW_ARTIFACT: self.handle_event__new_artifact(event_data = event_data)
        elif event_type == Flow_Run__Event_Type.TASK_START  : self.handle_event__task_start  (event_data = event_data)
        elif event_type == Flow_Run__Event_Type.TASK_STOP   : self.handle_event__task_stop   (event_data = event_data)
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

    def handle_event__new_artifact(self, event_data):
        artifact_data          = event_data.data.get('artifact_data')
        flow_run_id            = event_data.flow_run_id
        artifact_key           = artifact_data.get('key'        )  # add code to validate this value
        artifact_type          = artifact_data.get('type'       )
        artifact_description   = artifact_data.get('description')
        artifact_data          = artifact_data.get('data'       )
        prefect__flow_run_id   = self.prefect_ids_mapping.get(flow_run_id)

        kwargs                 = { "key"          : artifact_key         ,           # find a better name for this variable than kwargs
                                   "type"         : artifact_type        ,
                                   "description"  : artifact_description ,
                                   "data"         : artifact_data        ,
                                   "flow_run_id"  : prefect__flow_run_id }
        self.prefect_cloud_api.artifacts__create(kwargs)

    def handle_event__new_result(self, event_data):
        data                 = event_data.data.get('result_data')
        flow_run_id          = event_data.flow_run_id
        result_key           = data.get('key'        )        # add code to validate this value
        result_description   = data.get('description')
        prefect__flow_run_id = self.prefect_ids_mapping.get(flow_run_id)
        result_data          =  { "key"        : result_key                 ,
                                  "type"       : Prefect__Artifacts.RESULT  ,
                                  "description": result_description         ,
                                  "flow_run_id": prefect__flow_run_id       }
        self.prefect_cloud_api.artifacts__create(result_data)

    def handle_event__task_message(self, flow_event: Flow_Run__Event):
        event_data           = flow_event.event_data
        event_timestamp      = flow_event.timestamp
        flow_run_id          = event_data.flow_run_id
        task_run_id          = event_data.task_run_id
        message_data         = event_data.data.get('message_data')
        prefect__flow_run_id = self.prefect_ids_mapping.get(flow_run_id)
        prefect__task_run_id = self.prefect_ids_mapping.get(task_run_id)
        event_datetime       = timestamp_to_datetime(event_timestamp)
        prefect_timestamp    = self.prefect_cloud_api.to_prefect_timestamp(event_datetime)
        log_name = 'log-message'
        log_data = dict(flow_run_id = prefect__flow_run_id              ,
                        task_run_id = prefect__task_run_id              ,
                        level       = message_data.get('log_level'     ),
                        name        = log_name                          ,
                        message     = message_data.get('message_text',''),
                        timestamp   = prefect_timestamp                 )

        self.prefect_cloud_api.logs__create([log_data])

    def handle_event__flow_start(self, event_data: Flow_Run__Event_Data):
        flow_name    = event_data.flow_name
        flow_run_id  = event_data.flow_run_id
        prefect__flow_id                         = self.prefect_cloud_api.flow__create({'name': flow_name}).data.id
        tag__current_env                         = self.current_execution_environment()
        tag__current_time                        = time_now()
        prefect__flow_run_definition             = dict(flow_id    = prefect__flow_id                            ,
                                                        name       = flow_run_id                                     ,
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
            prefect__flow_run_id                  = prefect_flow_run.data.id
            self.prefect_ids_mapping[flow_name  ] = prefect__flow_id
            self.prefect_ids_mapping[flow_run_id] = prefect__flow_run_id
            self.prefect_cloud_api.flow_run__set_state_type__running(prefect__flow_run_id)

    def handle_event__flow_stop(self, event_data: Flow_Run__Event_Data):
        prefect__flow_run_id = self.prefect_ids_mapping.get(event_data.flow_run_id)
        self.prefect_cloud_api.flow_run__set_state_type__completed(prefect__flow_run_id)

    def handle_event__task_start(self, event_data: Flow_Run__Event_Data):
        flow_run_id                   = event_data.flow_run_id
        task_name                     = event_data.task_name
        task_run_id                   = event_data.task_run_id
        prefect__flow_run_id          = self.prefect_ids_mapping.get(flow_run_id)
        if prefect__flow_run_id is None:
            raise ValueError(f"in handle_event__task_start, could not find prefect__flow_run_id from flow_run_id: {flow_run_id}")
        prefect__task_run_definition  = { 'flow_run_id' : prefect__flow_run_id,
                                          'dynamic_key' : Random_Guid()       ,
                                          'task_key'    : Random_Guid()       ,
                                          'name'        : task_name           ,
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
            self.prefect_ids_mapping[task_run_id] = prefect__task_run_id            # capture mapping # todo: this will need a persistent layer, when running in pure S3
            self.prefect_cloud_api.task_run__set_state_type__running(prefect__task_run_id)

    def handle_event__task_stop(self, event_data: Flow_Run__Event_Data):
        task_run_id           = event_data.task_run_id
        prefect__task_run_id = self.prefect_ids_mapping.get(task_run_id)
        self.prefect_cloud_api.task_run__set_state_type__running__completed(prefect__task_run_id)

    def event_listener(self, flow_event: Flow_Run__Event):
        self.handle_event(flow_event=flow_event)

    def remove_event_listener(self):
        flow_events.event_listeners.remove(self.event_listener)