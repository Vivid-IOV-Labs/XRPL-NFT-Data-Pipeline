# TaxonPriceDump

The Taxon Price Dump class handles the extraction and storage of hourly pricing 
data for nft issuer projects(identified by the taxon and issuer). It creates an hourly dump of `floor_price`, `mid_price` and `volume`
for each tracked nft projects(identified by tracked issuers and their taxons) on s3.

## Output
This class creates two dumps. One for the pricing (mid_price and floor_price) and another for the project volume.
### Pricing Output
#### Location: `xls20-nft-project-price-tracker/{issuer}/{taxon}`
#### Object
```json
{
  "floor_price": "float",
  "mid_price": "float"
}
```
### Volume Output
#### Location: `xls20-nft-project-price-tracker/volume`
#### Object
```json
[
  {
    "issuer": "string",
    "taxon": "int", 
    "volume": "float"
  }
]
```
## Python Code
The code for this class can be found in [TaxonPriceDump](/sls_lambda/pricing.py) class.
___
# IssuerPriceDump

This creates an hourly dump of `floor_price`, `mid_price` and `maximum_buy_offer` of all the projects owned by each tracked issuer on xrpl.
The files are dumped to s3 bucket.

## Output
#### Location: `python-xls20-pricing/{hour}/{issuer}`
#### Object
```json
{
  "issuer": "string",
  "floor_price": "float",
  "max_buy_offer": "float",
  "mid_price": "float"
}
```
## Python Code
The code for this class can be found in [IssuerPriceDump](/sls_lambda/pricing.py) class.
