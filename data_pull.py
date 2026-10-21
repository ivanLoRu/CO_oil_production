# Databricks notebook source

import requests
import json
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LAYER_URL = ('https://gisdnr.state.co.us/arcgis/rest/services/ECMC_Public/Wells_Public_ECMC/MapServer/0')

# Retry temporary server errors and rate limits.
session = requests.Session()
session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
    ),
)

def get_json(url, params):
    if url.endswith("/query"):
        response = session.post(url, data=params, timeout=120)
    else:
        response = session.get(url, params=params, timeout=120)

    response.raise_for_status()

    result = response.json()
    if "error" in result:
        raise RuntimeError(result["error"])

    return result

# Read metadata to identify the record ID and request limit.
metadata = get_json(LAYER_URL, {"f": "json"})

id_field = next(
    field["name"]
    for field in metadata["fields"]
    if field["type"] == "esriFieldTypeOID"
)
page_size = min(metadata.get("maxRecordCount", 2000), 2000)

# First get the complete list of well record IDs.
id_result = get_json(
    f"{LAYER_URL}/query",
    {
        "where": "1=1",
        "returnIdsOnly": "true",
        "f": "json",
    },
)
object_ids = sorted(id_result.get("objectIds") or [])

# Retrieve attributes and coordinates in batches.
features = []

for start in range(0, len(object_ids), page_size):
    batch_ids = object_ids[start:start + page_size]

    result = get_json(
        f"{LAYER_URL}/query",
        {
            "objectIds": ",".join(map(str, batch_ids)),
            "outFields": "*",
            "returnGeometry": "true",
            # Longitude and latitude
            "outSR": 4326,
            "f": "json",
        },
    )

    batch_features = result.get("features", [])

    if result.get("exceededTransferLimit"):
        raise RuntimeError("Response was truncated; reduce page_size.")

    features.extend(batch_features)
    print(f"Downloaded {len(features):,} / {len(object_ids):,} wells")

# Verify that every requested record was returned exactly once.
returned_ids = [feature["attributes"][id_field] for feature in features]

if (
    len(returned_ids) != len(object_ids)
    or set(returned_ids) != set(object_ids)
):
    raise RuntimeError("Downloaded records do not match requested IDs.")

# Save to jsonl, this isn't part of databricks notebook

output_path = Path("data/raw/colorado_wells.jsonl")
output_path.parent.mkdir(parents=True, exist_ok=True)

with output_path.open("w", encoding="utf-8") as file:
    for feature in features:
        file.write(json.dumps(feature) + "\n")