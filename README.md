# MLB Roadtrip

## Populate Elasticsearch Data

### Get the raw data

Requires bs4 python3 library. TODO Convert this script to go.
`$ python3 scripts/get_game_data.py`

### Start Elasticsearch locally

`docker-compose up`

### Apply mappings and add data

`go run scripts/load_es.go`

## Installation

Glide is used for dependency management. [https://github.com/Masterminds/glide]
`glide install`
