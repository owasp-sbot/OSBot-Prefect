from unittest                                            import TestCase

from osbot_utils.utils.Misc import time_now

from osbot_utils.helpers.flows.models.Flow__Config       import Flow__Config
from osbot_utils.utils.Env                               import load_dotenv
from osbot_utils.helpers.flows.decorators.task           import task
from osbot_utils.helpers.flows.Flow                      import Flow
from osbot_utils.helpers.flows.decorators.flow           import flow
from osbot_prefect.flows.Flow_Events__To__Prefect_Server import Flow_Events__To__Prefect_Server


class test_Flow_Events__To__Prefect_Server(TestCase):

    def setUp(self):
        load_dotenv()
        self.flows_to_prefect = Flow_Events__To__Prefect_Server()

    def test_event_listener(self):
        self.flows_to_prefect.add_event_listener()
        test_flow         = an_flow_1()
        test_flow.flow_id = time_now(milliseconds_numbers=0)
        test_flow.execute_flow()
        test_flow.log_error("Testing an error")
        self.flows_to_prefect.remove_event_listener()


# example flow
flow_config = Flow__Config(logging_enabled=False)

@flow(flow_config=flow_config)
def an_flow_1() -> Flow:
    print('inside the flow')
    an_task_1()


@task()
def an_task_1():
    print('inside the task')