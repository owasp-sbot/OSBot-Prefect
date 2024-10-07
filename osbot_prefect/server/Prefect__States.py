from osbot_utils.base_classes.Type_Safe import Type_Safe


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