import logging
import os
from pathlib import Path

import requests
from pystac import Item

from open_cosmos_adaptor.auth_utils import get_access_token


def download_and_store_locally(stac_item: Item, destination_folder: Path, workspace: str) -> None:
    """Download and store ordered asset files to a local folder."""

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    headers = {"Authorization": f"Bearer {get_access_token(workspace)}"}

    for asset in stac_item.assets.values():
        filename = os.path.basename(asset.href)
        destination_path = destination_folder / filename

        logging.info(f"Downloading asset from {asset.href} to {destination_path}")

        try:
            response = requests.get(asset.href, stream=True, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(f"Failed to download asset {asset.href} with error: {e}")
            continue

        try:
            with open(destination_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            asset.href = str(destination_path)
            logging.info(f"Successfully downloaded {asset.href} to {filename}")
        except requests.exceptions.ChunkedEncodingError as e:
            logging.error(f"Failed to download asset {asset.href} with error: {e}")
            continue
