import json
import datetime


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_issuer_taxons(issuer, environment):
    if environment == "LOCAL":
        taxons = json.load(open("data/nfts/taxon.json", "r"))
        return [taxon["taxons"] for taxon in taxons if taxon["issuer"] == issuer][0]


def fetch_issuer_tokens(issuer, environment):
    last_hour = datetime.datetime.now().strftime('%Y-%m-%d-%H')
    # last_hour = "2022-12-11-13"
    if environment == "LOCAL":
        data = json.load(open(f"data/nfts/{last_hour}/{issuer}.json", "r"))
        return data["nfts"]
