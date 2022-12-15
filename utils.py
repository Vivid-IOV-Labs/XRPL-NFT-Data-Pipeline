import json
import datetime
import boto3


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
        print(f"{last_hour}/{issuer}.json")
        client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        result = client.get_object(Bucket=bucket, Key=f"{last_hour}/{issuer}.json")

        text = result["Body"].read()
        return json.loads(text.decode())["nfts"]

