import logging

import aioboto3
import boto3
import botocore
from utilities import Config

logger = logging.getLogger("app_log")


def invoke_csv_dump(config: Config):
    lambda_client = boto3.client(
        "lambda",
        region_name="eu-west-2",
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    resp = lambda_client.invoke(
        FunctionName=f"csv-dump-{config.STAGE}",
        InvocationType="Event",
    )
    logger.info(resp)


async def invoke_token_dumps(config: Config):
    session = aioboto3.Session(  # noqa
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
        region_name="eu-west-2",
    )
    b_config = botocore.config.Config(read_timeout=900)
    async with session.client("lambda", config=b_config) as lambda_client:
        await lambda_client.invoke(
            FunctionName=f"issuers-nft-dump-{config.STAGE}",
            InvocationType="RequestResponse",
        )


async def invoke_taxon_dumps(config: Config):
    logger.info("Invoking Taxon Dumps")
    session = aioboto3.Session(  # noqa
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
        region_name="eu-west-2",
    )
    b_config = botocore.config.Config(read_timeout=900)
    async with session.client("lambda", config=b_config) as lambda_client:
        await lambda_client.invoke(
            FunctionName=f"issuers-taxon-dump-{config.STAGE}",
            InvocationType="RequestResponse",
        )


async def invoke_table_dump(config: Config):
    session = aioboto3.Session(  # noqa
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
        region_name="eu-west-2",
    )
    b_config = botocore.config.Config(read_timeout=900)
    async with session.client("lambda", config=b_config) as lambda_client:
        await lambda_client.invoke(
            FunctionName=f"nft-table-dump-{config.STAGE}",
            InvocationType="Event",
        )


async def invoke_graph_dump(config: Config):
    session = aioboto3.Session(  # noqa
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
        region_name="eu-west-2",
    )
    b_config = botocore.config.Config(read_timeout=900)
    async with session.client("lambda", config=b_config) as lambda_client:
        await lambda_client.invoke(
            FunctionName=f"nft-graph-dump-{config.STAGE}",
            InvocationType="Event",
        )


async def invoke_twitter_dump(config: Config):
    session = aioboto3.Session(  # noqa
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
        region_name="eu-west-2",
    )
    b_config = botocore.config.Config(read_timeout=900)
    async with session.client("lambda", config=b_config) as lambda_client:
        await lambda_client.invoke(
            FunctionName=f"nft-twitter-dump-{config.STAGE}",
            InvocationType="Event",
        )
