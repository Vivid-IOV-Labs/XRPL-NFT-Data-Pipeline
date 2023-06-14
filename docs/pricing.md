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
  "floor_price": "calculated floor price",
  "mid_price": "calculated mid price"
}
```
### Volume Output
#### Location: `xls20-nft-project-price-tracker/volume`
#### Object
```json
[
  {
    "issuer": "project issuer",
    "taxon": "project taxon", 
    "volume": "calculated project volume"
  }
]
```
## Python Code
The code for this class can be found in [TaxonPriceDump](/sls_lambda/pricing.py) class.

