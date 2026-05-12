import logging
import os
from pathlib import Path

import requests
from pystac import Item

from open_cosmos_adaptor.auth_utils import get_access_token


def download_and_store_locally(stac_item: Item, parent_folder: Path, destination_folder: Path) -> None:
    """Download and store order files to a local folder"""
    # Create the destination folder if it doesn't exist
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    headers = {
        "Authorization": f"Bearer {get_access_token()}"
    }

    for asset in stac_item.assets.values():
        break

        filename = os.path.basename(asset.href)
        destination_path = destination_folder / filename

        logging.info(f"Downloading asset from {asset.href} to {destination_path}")

        response = requests.get(asset.href, stream=True, headers=headers)
        response.raise_for_status()

        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        asset.href = str(destination_path)
        logging.info(f"Successfully downloaded {filename}")
