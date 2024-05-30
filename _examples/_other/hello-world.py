from prefect import flow, task

@task
def say_hello(name: str):
    print(f"hello {name}!")


@flow(name="Hello World!", log_prints=True)
def hello_world(name: str = "world"):
    """Given a name, say hello to them!"""
    say_hello(name=name)


if __name__ == "__main__":
    hello_world(name="Will")