import asyncio

from datetime import datetime
from prefect import flow, pause_flow_run, get_run_logger
from prefect.input import RunInput


class UserInput(RunInput):
    name: str
    age: int


@flow
async def greet_user():
    logger = get_run_logger()
    current_date = datetime.now().strftime("%B %d, %Y")

    description_md = f"""
**Welcome to the User Greeting Flow!**
Today's Date: {current_date}

Please enter your details below:
- **Name**: What should we call you?
- **Age**: Just a number, nothing more.
"""

    user_input = await pause_flow_run(
        wait_for_input=UserInput.with_initial_data(
            description=description_md, name="anonymous"
        )
    )

    if user_input.name == "anonymous":
        logger.info("Hello, strangaer!")
    else:
        logger.info(f"Hello, {user_input.name}!")

if __name__ == "__main__":
    asyncio.run(greet_user())