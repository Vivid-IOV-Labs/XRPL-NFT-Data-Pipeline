# import json
# import logging
#
# import aioboto3
# import boto3
# import botocore
#
# from config import Config
#
# logger = logging.getLogger("app_log")
#
#
# async def invoke_token_pricing_dump(issuer):
#     session = aioboto3.Session(  # noqa
#         aws_access_key_id=Config.ACCESS_KEY_ID,
#         aws_secret_access_key=Config.SECRET_ACCESS_KEY,
#         region_name="eu-west-2",
#     )
#     config = botocore.config.Config(read_timeout=900)
#     async with session.client("lambda", config=config) as lambda_client:
#         await lambda_client.invoke(
#             FunctionName=f"issuer-token-price-dumps-{Config.STAGE}",
#             InvocationType="RequestResponse",
#             Payload=json.dumps({"issuer": issuer}).encode("utf-8"),
#         )
#
#
# async def invoke_issuer_price_dump(issuer):
#     session = aioboto3.Session(  # noqa
#         aws_access_key_id=Config.ACCESS_KEY_ID,
#         aws_secret_access_key=Config.SECRET_ACCESS_KEY,
#         region_name="eu-west-2",
#     )
#     config = botocore.config.Config(read_timeout=900)
#     async with session.client("lambda", config=config) as lambda_client:
#         await lambda_client.invoke(
#             FunctionName=f"issuer-price-dump-{Config.STAGE}",
#             InvocationType="RequestResponse",
#             Payload=json.dumps({"issuer": issuer}).encode("utf-8"),
#         )
#
#
# def invoke_issuers_pricing_dump():
#     lambda_client = boto3.client(
#         "lambda",
#         region_name="eu-west-2",
#         aws_access_key_id=Config.ACCESS_KEY_ID,
#         aws_secret_access_key=Config.SECRET_ACCESS_KEY,
#     )
#     resp = lambda_client.invoke(
#         FunctionName=f"tracked-issuers-price-dumps-{Config.STAGE}",
#         InvocationType="Event",
#     )
#     logger.info(resp)
#
#
# def invoke_csv_dump():
#     lambda_client = boto3.client(
#         "lambda",
#         region_name="eu-west-2",
#         aws_access_key_id=Config.ACCESS_KEY_ID,
#         aws_secret_access_key=Config.SECRET_ACCESS_KEY,
#     )
#     resp = lambda_client.invoke(
#         FunctionName=f"csv-dump-{Config.STAGE}",
#         InvocationType="Event",
#     )
#     logger.info(resp)
#
#
# async def invoke_table_dump():
#     session = aioboto3.Session(  # noqa
#         aws_access_key_id=Config.ACCESS_KEY_ID,
#         aws_secret_access_key=Config.SECRET_ACCESS_KEY,
#         region_name="eu-west-2",
#     )
#     config = botocore.config.Config(read_timeout=900)
#     async with session.client("lambda", config=config) as lambda_client:
#         await lambda_client.invoke(
#             FunctionName=f"nft-table-dump-{Config.STAGE}",
#             InvocationType="Event",
#         )
#
#
# async def invoke_graph_dump():
#     session = aioboto3.Session(  # noqa
#         aws_access_key_id=Config.ACCESS_KEY_ID,
#         aws_secret_access_key=Config.SECRET_ACCESS_KEY,
#         region_name="eu-west-2",
#     )
#     config = botocore.config.Config(read_timeout=900)
#     async with session.client("lambda", config=config) as lambda_client:
#         await lambda_client.invoke(
#             FunctionName=f"nft-graph-dump-{Config.STAGE}",
#             InvocationType="Event",
#         )
#
#
# async def invoke_twitter_dump():
#     session = aioboto3.Session(  # noqa
#         aws_access_key_id=Config.ACCESS_KEY_ID,
#         aws_secret_access_key=Config.SECRET_ACCESS_KEY,
#         region_name="eu-west-2",
#     )
#     config = botocore.config.Config(read_timeout=900)
#     async with session.client("lambda", config=config) as lambda_client:
#         await lambda_client.invoke(
#             FunctionName=f"nft-twitter-dump-{Config.STAGE}",
#             InvocationType="Event",
#         )
