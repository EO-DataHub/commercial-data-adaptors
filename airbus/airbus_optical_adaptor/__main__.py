import argparse
import json
import logging
import os
from typing import Dict, List, Optional

from airbus_optical_adaptor.api_utils import post_submit_order
from common.s3_utils import download_and_store_locally, poll_s3_for_data
from common.stac_utils import (
    OrderStatus,
    get_item_hrefs_from_catalogue,
    get_key_from_stac,
    retrieve_stac_item,
    update_stac_item_failure,
    update_stac_item_ordered,
    update_stac_item_success,
    verify_coordinates,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


product_bundle_map = {
    "Visual": {
        "productBundle": "visual",
        "processingLevel": "ortho",
        "pixelCoding": "8bits",
        "radiometricProcessing": "display",
        "spectralProcessing": "pansharpened_natural_color",
        "dem": "best_available",
        "projection": True,
    },
    "General Use": {
        "productBundle": "general",
        "processingLevel": "ortho",
        "pixelCoding": "12bits",
        "radiometricProcessing": "reflectance",
        "spectralProcessing": "pansharpened",
        "dem": "best_available",
        "projection": True,
    },
    "Analytic": {
        "productBundle": "analytic",
        "processingLevel": "ortho",
        "pixelCoding": "12bits",
        "radiometricProcessing": "reflectance",
        "spectralProcessing": "bundle",
        "dem": "best_available",
        "projection": True,
    },
    "Basic": {
        "productBundle": "basic",
        "processingLevel": "primary",
        "pixelCoding": "12bits",
        "radiometricProcessing": "basic",
        "spectralProcessing": "bundle",
    },
}


class STACItem:
    """Class to represent a STAC item and its properties"""

    def __init__(self, stac_item_path: str):
        self.file_path = stac_item_path
        self.file_name = os.path.basename(stac_item_path)
        self.stac_json = retrieve_stac_item(stac_item_path)
        self.acquisition_id = get_key_from_stac(
            self.stac_json, "properties.acquisition_identifier"
        )
        self.collection_id = get_key_from_stac(self.stac_json, "collection")
        self.coordinates = get_key_from_stac(self.stac_json, "geometry.coordinates")
        self.multi_acquisition_ids = (
            get_key_from_stac(
                self.stac_json, "properties.composed_of_acquisition_identifiers"
            )
            or []
        )
        self.order_status = get_key_from_stac(self.stac_json, "order:status")
        self.item_uuid = get_key_from_stac(self.stac_json, "properties.id")
        self.item_uuids = []


def prepare_stac_items_to_order(catalogue_dirs: List[str]) -> List[STACItem]:
    """Prepare a list of STAC items to order, including multi-acquisition items"""
    stac_item_paths = []
    for catalogue_dir in catalogue_dirs:
        if not os.path.exists(catalogue_dir):
            raise FileNotFoundError(f"Catalogue directory {catalogue_dir} not found.")
        stac_item_paths += get_item_hrefs_from_catalogue(catalogue_dir)
    if not stac_item_paths:
        raise ValueError("No STAC items found in the given directories.")
    acquisition_id_to_path = {
        os.path.splitext(os.path.basename(path))[0]: path for path in stac_item_paths
    }
    logging.info(f"STAC item paths: {stac_item_paths}")

    stac_items = []

    for stac_item_path in stac_item_paths:
        stac_item_to_add = STACItem(stac_item_path)
        # Do not add the item if it is already part of a multi-acquisition item
        if any(
            stac_item_to_add.acquisition_id in stac_item.multi_acquisition_ids
            for stac_item in stac_items
        ):
            continue
        if stac_item_to_add.multi_acquisition_ids:
            logging.info(
                f"Item {stac_item_to_add.acquisition_id} is a multi-acquisition item"
            )
            logging.info(
                f"Multi-acquisition IDs: {stac_item_to_add.multi_acquisition_ids}"
            )
            for multi_acquisition_id in stac_item_to_add.multi_acquisition_ids:
                # The order is incomplete if not all multi-acquisition items are present
                multi_stac_item_path = acquisition_id_to_path.get(multi_acquisition_id)
                if not multi_stac_item_path:
                    raise ValueError(
                        f"File {multi_acquisition_id} not found in given ids: {acquisition_id_to_path}"
                    )
                # Add the UUID of each item to the main multi-acquisition item
                multi_stac_item = STACItem(multi_stac_item_path)
                stac_item_to_add.item_uuids.append(multi_stac_item.item_uuid)
            # Remove the multi-acquisition items from the list
            stac_items = [
                item
                for item in stac_items
                if item.acquisition_id not in stac_item_to_add.multi_acquisition_ids
            ]
        else:
            stac_item_to_add.item_uuids = [stac_item_to_add.item_uuid]
        stac_items.append(stac_item_to_add)

    return stac_items


def get_order_options(product_bundle: str) -> dict:
    """Return the order options for the given product bundle"""
    available_bundles = ["General Use", "Visual", "Analytic", "Basic"]
    if product_bundle not in available_bundles:
        raise NotImplementedError(
            f"Product bundle {product_bundle} is not valid. Currently implemented bundles are {available_bundles}"
        )
    return product_bundle_map[product_bundle]


def main(
    workspace: str,
    workspace_bucket: str,
    commercial_data_bucket: str,
    pulsar_url: str,
    product_bundle: str,
    coordinates: List,
    catalogue_dirs: List[str],
    licence: str,
    end_users: Optional[List[Dict[str, str]]] = None,
):
    """Submit an order for an acquisition, retrieve the data, and update the STAC item"""
    # Workspace STAC item should already be generated and ingested, with an order status of ordered.
    logging.info(f"Ordering items in catalogues from stage in: {catalogue_dirs}")
    order_options = get_order_options(product_bundle)
    logging.info(f"Order options: {order_options}")
    stac_items: List[STACItem] = prepare_stac_items_to_order(catalogue_dirs)
    logging.info(f"Coordinates: {coordinates}")
    if not verify_coordinates(coordinates):
        raise ValueError(f"Invalid coordinates: {coordinates}")
    logging.info(f"Target workspace: {workspace}")

    for stac_item in stac_items:
        try:
            acquisition_id = stac_item.acquisition_id
            # Submit an order for the given STAC item
            logging.info(f"Ordering STAC item {acquisition_id}")
            if stac_item.order_status == OrderStatus.ORDERED.value:
                reason = f"Order for {acquisition_id} is already in progress"
                logging.error(reason)
                # Unable to obtain the item_id again, so cannot wait for data. Fail the order.
                update_stac_item_failure(
                    stac_item.stac_json,
                    stac_item.file_name,
                    stac_item.collection_id,
                    reason,
                    workspace,
                    workspace_bucket,
                )
                return
            if not coordinates:
                # Limit order by an AOI if provided
                coordinates = stac_item.coordinates
            order_id, customer_reference = post_submit_order(
                acquisition_id,
                stac_item.collection_id,
                coordinates,
                order_options,
                workspace,
                licence,
                stac_item.item_uuids,
                end_users,
            )
        except Exception as e:
            reason = f"Failed to submit order: {e}"
            logging.error(reason, exc_info=True)
            update_stac_item_failure(
                stac_item.stac_json,
                stac_item.file_name,
                stac_item.collection_id,
                reason,
                workspace,
                workspace_bucket,
            )
            return
        # Update the STAC record after submitting the order
        update_stac_item_ordered(
            stac_item.stac_json,
            stac_item.collection_id,
            stac_item.file_name,
            order_id,
            workspace_bucket,
            pulsar_url,
            workspace,
        )
        try:
            # Wait for data from airbus to arrive, then download it
            # Archive is of the format <customer_reference>_<internal_reference>_<acquisition_id>.zip
            objs = poll_s3_for_data(
                commercial_data_bucket,
                customer_reference,
                f"{acquisition_id}.zip",
            )
            for obj in objs:
                download_and_store_locally(
                    commercial_data_bucket,
                    obj,
                    customer_reference,
                )
        except Exception as e:
            reason = f"Failed to retrieve data: {e}"
            logging.error(reason, exc_info=True)
            update_stac_item_failure(
                stac_item.stac_json,
                stac_item.file_name,
                stac_item.collection_id,
                reason,
                workspace,
                workspace_bucket,
                order_id,
            )
            return
        update_stac_item_success(
            stac_item.stac_json,
            stac_item.file_name,
            stac_item.collection_id,
            order_id,
            customer_reference,
            workspace,
            workspace_bucket,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order Airbus data")
    parser.add_argument("workspace", type=str, help="Workspace name")
    parser.add_argument("workspace_bucket", type=str, help="Workspace bucket")
    parser.add_argument(
        "commercial_data_bucket", type=str, help="Commercial data bucket"
    )
    parser.add_argument("pulsar_url", type=str, help="Pulsar URL")
    parser.add_argument("product_bundle", type=str, help="Product bundle")
    parser.add_argument(
        "--coordinates", type=str, required=True, help="Stringified list of coordinates"
    )
    parser.add_argument(
        "--catalogue_dirs",
        nargs="+",
        required=True,
        help="List of catalogue directories",
    )
    parser.add_argument(
        "--end_users",
        type=str,
        required=True,
        help="Stringified list of end user names and countries",
    )
    parser.add_argument(
        "--licence",
        type=str,
        required=True,
        help="Licence used for the order",
    )

    args = parser.parse_args()

    coordinates = json.loads(args.coordinates)
    end_users = json.loads(args.end_users) if args.end_users else None

    main(
        args.workspace,
        args.workspace_bucket,
        args.commercial_data_bucket,
        args.pulsar_url,
        args.product_bundle,
        coordinates,
        args.catalogue_dirs,
        args.licence,
        end_users,
    )
