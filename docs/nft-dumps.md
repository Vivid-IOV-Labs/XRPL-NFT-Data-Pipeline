# NFTokenDump

The NFToken dump creates an hourly s3 dump of all the nft tokens issued by each peerkat's tracked issuer and their respective taxons. It also gives information on
 the circulation, number of holders and the supply of each nft_tokens.

## Output
This class creates two dumps. One for the nft tokens and another for the supply of each tracked issuer.
### NFT Token Dumps Output
#### Location: `xls20-issuer-nfts/{hour}/{issuer}.json`
#### Object
```json
{
    "issuer": "string",
    "nfts": [
      {
        "NFTokenID": "string",
        "Issuer": "string",
        "Owner": "string",
        "Taxon": "integer",
        "Sequence": "integer",
        "TransferFee": "integer",
        "Flags": "integer",
        "URI": "string"
      }
    ]
}
```
### Supply Output
#### Location: `xls20-issuer-nfts/supply.json`
#### Object
An array containing supply object for each tracked issuer
```json
[
 {
  "issuer": "string", 
  "supply": "integer", 
  "circulation": "integer", 
  "holders": "integer",
  "tokens_held": "integer"
 }
]
```
## Python Code
The code for this class can be found in [NFTokenDump](/sls_lambda/nft_dumps.py) class.
___
# NFTaxonDump

This creates an s3 dump of all known taxons used by each of peerkat's tracked issuer.

## Output
#### Location: `xls20-issuer-nfts/taxon.json'
#### Object
```json
[
 {
  "issuer": "string",
  "taxons": "Array of all currently known Taxons used by the given Issuer"
 }
]
```
## Python Code
The code for this class can be found in [NFTaxonDump](/sls_lambda/nft_dumps.py) class.