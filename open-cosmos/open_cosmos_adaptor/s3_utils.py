import logging
import os
from pathlib import Path

import boto3
import requests
from pystac import Item

from open_cosmos_adaptor.auth_utils import get_access_token


def download_and_store_locally(collection_id: str, stac_item: Item, parent_folder: Path, destination_folder: Path) -> None:
    """Download and store ordered asset files to a local folder."""

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    headers = {"Authorization": f"Bearer {get_access_token(collection_id)}"}

    for asset in stac_item.assets.values():
        filename = os.path.basename(asset.href)
        destination_path = destination_folder / filename

        logging.info(f"Downloading asset from {asset.href} to {destination_path}")

        response = requests.get(asset.href, stream=True, headers=headers)
        response.raise_for_status()

        with open(destination_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        asset.href = str(destination_path)
        logging.info(f"Successfully downloaded {filename}")


def upload_to_s3(stac_item: Item, source_folder: Path, s3_bucket: str, s3_key: str) -> None:
    """Upload the ordered assets to their final resting place on S3."""
    logging.info(f"Uploading STAC item to S3: {s3_key}")
    s3_client = boto3.client("s3")

    for asset in stac_item.assets.values():
        filename = os.path.basename(asset.href)
        s3_client.upload_file(
            str(source_folder / str(filename)),
            s3_bucket,
            s3_key,
        )
