import json
import datetime
import boto3
import aioboto3
import snscrape.modules.twitter as twitter_scrapper


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_issuer_taxons(issuer, environment, bucket, access_key, secret_key):
    if environment == "LOCAL":
        taxons = json.load(open("data/nfts/taxon.json", "r"))
        return [taxon["taxons"] for taxon in taxons if taxon["issuer"] == issuer][0]
    else:
        client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        result = client.get_object(Bucket=bucket, Key="taxon.json")
        text = result["Body"].read()
        taxons = json.loads(text.decode())
        return [taxon["taxons"] for taxon in taxons if taxon["issuer"] == issuer][0]



def fetch_issuer_tokens(issuer, environment, bucket, access_key, secret_key):
    last_hour = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H')
    # last_hour = "2022-12-11-13"
    if environment == "LOCAL":
        last_hour = "2022-12-11-13"
        data = json.load(open(f"data/nfts/{last_hour}/{issuer}.json", "r"))
        return data["nfts"]
    else:
        client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        result = client.get_object(Bucket=bucket, Key=f"{last_hour}/{issuer}.json")

        text = result["Body"].read()
        return json.loads(text.decode())["nfts"]


async def read_json(bucket, key, config):
    session = aioboto3.Session(aws_access_key_id=config.ACCESS_KEY_ID,
                               aws_secret_access_key=config.SECRET_ACCESS_KEY, )
    async with session.client("s3") as s3:
        try:
            res = await s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(e)
            return None
        body = res["Body"]
        data = await body.read()
        return json.loads(data)


def to_snake_case(col: str):
    return "_".join([x.lower() for x in col.split("_")])


def twitter_pics(name):
    try:
        pic = twitter_scrapper.TwitterUserScraper(name)._get_entity()  # noqa
        return pic.profileImageUrl.replace("normal", "400x400"), pic.profileBannerUrl + "/1500x500"
    except Exception as e:
        print(e)
        return "", ""
