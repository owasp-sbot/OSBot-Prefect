import os


def in_aws_lambda():
    return "AWS_LAMBDA_FUNCTION_NAME" in os.environ