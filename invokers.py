import json
import aioboto3
import botocore
from config import Config


async def invoke_token_pricing_dump(issuer):
    session = aioboto3.Session(
        aws_access_key_id=Config.ACCESS_KEY_ID,
        aws_secret_access_key=Config.SECRET_ACCESS_KEY,
        region_name="eu-west-2"
    )
    config = botocore.config.Config(read_timeout=900)
    async with session.client("lambda", config=config) as lambda_client:
        await lambda_client.invoke(
            FunctionName=f"issuer-token-price-dumps-{Config.STAGE}",
            InvocationType="RequestResponse",
            Payload=json.dumps({"issuer": issuer}).encode('utf-8')
        )
