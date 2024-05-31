from prefect import flow, pause_flow_run, get_run_logger

@flow
def greet_user():
    logger = get_run_logger()

    user = pause_flow_run(wait_for_input=str)

    logger.info(f"Hello, {user}!")


if __name__ == "__main__":
    greet_user()