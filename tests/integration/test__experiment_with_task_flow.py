from unittest                                           import TestCase
from osbot_utils.helpers.flows.decorators.task          import task
from osbot_utils.helpers.flows.models.Flow_Run__Config  import Flow_Run__Config
from osbot_utils.helpers.flows.Flow                     import Flow
from osbot_utils.helpers.flows.decorators.flow          import flow


class test__experiment_with_task_flow(TestCase):

    def test_task_and_flow(self):
        flow_config = Flow_Run__Config(logging_enabled=False)

        @task()
        def a_task():
            print('in a task')

        @flow(flow_config=flow_config)
        def an_flow() -> Flow:
            print('in a flow')
            a_task()

        flow_1 = an_flow()
        flow_1.execute_flow()
