import json
import logging
import mimetypes
import os
from datetime import datetime, timezone
from enum import Enum
import re
from typing import List, Union

import boto3
import pulsar

Coordinate = Union[List[float], tuple[float, float]]


class OrderStatus(Enum):
    ORDERABLE = "orderable"
    ORDERED = "ordered"
    PENDING = "pending"
    SHIPPING = "shipping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


def retrieve_stac_item(file_path: str) -> dict:
    """Retrieve a STAC item from a local JSON file"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, "r", encoding="utf-8") as f:
        stac_item = json.load(f)
    return stac_item


def current_time_iso8601() -> str:
    """Return the current time in ISO 8601 format"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def write_stac_item_and_catalog(
    stac_item: dict,
    stac_item_filename: str,
    collection_id: str,
    item_id: str,
    workspace: str,
    workspaces_bucket: str,
):
    """Creates local catalog containing final STAC item to be used as a record for the order"""
    # Rewrite STAC links to point to local files only
    stac_item["links"] = [
        {"rel": "self", "href": stac_item_filename, "type": "application/json"},
        {"rel": "parent", "href": "catalog.json", "type": "application/json"},
    ]

    # Write the STAC item to a file
    with open(stac_item_filename, "w") as f:
        json.dump(stac_item, f, indent=2)
    logging.info(f"Created STAC item '{stac_item_filename}' locally.")
    logging.info(f"STAC item: {stac_item}")

    # If not item_id, the order has failed
    if not item_id:
        item_id = "Failed"

    # Create containing STAC catalog
    try:
        # obtain the existing catalog from s3 if possible
        s3_client = boto3.client("s3")
        key = f"{workspace}/commercial-data/airbus.json"
        logging.info(f"Retrieving existing catalog from s3: {key}, {workspaces_bucket}")
        response = s3_client.get_object(Bucket=workspaces_bucket, Key=key)
        stac_catalog = json.loads(response["Body"].read())

    except Exception as e:
        logging.info(f"Failed to retrieve existing collection from s3: {e}")
        logging.info("Creating default collection")
        stac_catalog = {
            "stac_version": "1.0.0",
            "id": "airbus",
            "type": "Catalog",
            "description": "Order records for Airbus, including completed purchases with their associated assets, as well as records of ongoing and failed orders.",
            "links": [],
        }
    stac_catalog["links"] = [
        {"rel": "self", "href": "catalog.json", "type": "application/json"},
        {"rel": "child", "href": "collection.json", "type": "application/json"},
    ]

    # Write the STAC catalog to a file
    with open("catalog.json", "w") as f:
        json.dump(stac_catalog, f, indent=2)
    logging.info("Created STAC catalog catalog.json locally.")
    logging.info(f"STAC catalog: {stac_catalog}")

    try:
        # obtain the existing collection from s3 if possible
        s3_client = boto3.client("s3")
        key = f"{workspace}/commercial-data/airbus/{collection_id}.json"
        logging.info(
            f"Retrieving existing collection from s3: {key}, {workspaces_bucket}"
        )
        response = s3_client.get_object(Bucket=workspaces_bucket, Key=key)
        stac_collection = json.loads(response["Body"].read())

    except Exception as e:
        logging.info(f"Failed to retrieve existing collection from s3: {e}")
        logging.info("Creating default collection")
        stac_collection = {
            "stac_version": "1.0.0",
            "id": collection_id,
            "type": "Collection",
            "description": f"Order records for {collection_id.capitalize().replace('_', ' ')}, including completed purchases with their associated assets, as well as records of ongoing and failed orders.",
            "license": "proprietary",
            "links": [],
            "keywords": ["airbus"],
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {
                    "interval": [
                        [
                            "2010-01-01T00:00:00Z",
                            current_time_iso8601(),
                        ]
                    ]
                },
            },
        }
    stac_collection["links"] = [
        {"rel": "self", "href": "collection.json", "type": "application/json"},
        {"rel": "root", "href": "catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "catalog.json", "type": "application/json"},
        {"rel": "item", "href": stac_item_filename, "type": "application/json"},
    ]

    # Write the STAC catalog to a file
    with open("collection.json", "w") as f:
        json.dump(stac_collection, f, indent=2)
    logging.info("Created STAC collection collection.json locally.")
    logging.info(f"STAC collection: {stac_collection}")


def update_stac_order_status(stac_item: dict, order_id: str, order_status: str):
    """Update the STAC item with the order status using the STAC Order extension"""
    # Update or add fields relating to the order
    if "properties" not in stac_item:
        stac_item["properties"] = {}

    if order_id is not None:
        stac_item["properties"]["order:id"] = order_id
    stac_item["properties"]["order:status"] = order_status

    # Update or add the STAC extension if not already present
    order_extension_url = "https://stac-extensions.github.io/order/v1.1.0/schema.json"
    if "stac_extensions" not in stac_item:
        stac_item["stac_extensions"] = []

    if order_extension_url not in stac_item["stac_extensions"]:
        stac_item["stac_extensions"].append(order_extension_url)


def get_key_from_stac(stac_item: dict, key: str):
    """Extract a nested key from a STAC item. Key given as a dot-separated string."""
    parts = key.split(".")
    value = stac_item
    for part in parts:
        value = value.get(part)
        if value is None:
            logging.info(f"{part} not found in STAC item.")
            return None
    logging.info(f"Retrieved {key} from STAC item: {value}")
    return value


def ingest_stac_item(
    stac_item: dict,
    s3_bucket: str,
    pulsar_url: str,
    workspace: str,
    collection_id: str,
    item_id: str,
):
    """Ingest the STAC item to the S3 bucket and send a Pulsar message"""
    # Upload the STAC item to S3
    s3_client = boto3.client("s3")
    parent_catalog_name = "commercial-data"

    item_key = (
        f"{workspace}/{parent_catalog_name}/airbus/{collection_id}/{item_id}.json"
    )
    s3_client.put_object(Body=json.dumps(stac_item), Bucket=s3_bucket, Key=item_key)

    logging.info(
        f"Uploaded STAC item to S3 bucket '{s3_bucket}' with key '{item_key}'."
    )

    transformed_item_key = (
        f"transformed/catalogs/user/catalogs/{workspace}/catalogs/{parent_catalog_name}/catalogs/"
        f"airbus/collections/{collection_id}/items/{item_id}.json"
    )
    s3_client.put_object(
        Body=json.dumps(stac_item), Bucket=s3_bucket, Key=transformed_item_key
    )

    logging.info(
        f"Uploaded STAC item to S3 bucket '{s3_bucket}' with key '{transformed_item_key}'."
    )

    # Send a Pulsar message
    pulsar_client = pulsar.Client(pulsar_url)
    producer = pulsar_client.create_producer(
        topic="transformed", producer_name=f"data_adaptor-{workspace}-{item_id}"
    )
    output_data = {
        "id": f"{workspace}/update_order",
        "workspace": workspace,
        "bucket_name": s3_bucket,
        "added_keys": [],
        "updated_keys": [transformed_item_key],
        "deleted_keys": [],
        "source": "/",
        "target": "/",
    }
    producer.send((json.dumps(output_data)).encode("utf-8"))
    logging.info(f"Sent Pulsar message {output_data}.")

    # Close the Pulsar client
    pulsar_client.close()

def get_asset_details(file_path: str, collection_id: str) -> tuple[str, str]:
    """
    Returns a tuple (name, description) if a match is found, otherwise (file_base_name, "").
    """
    regex_patterns = {
        "airbus_sar_data": [
            # Imagery
            (r"imagedata\/[^\\:?\"<>|]+\.(cos|tif|tiff)$", "primaryAsset", "GeoTIFF image file"),
            (r"imagedata\/[^\\:?\"<>|]+\.(cos)$", "primaryAsset", "COSAR binary image file"),
            (r"preview\/map_plot\.png$", "mapPlot", "A coarse geographical map showing the footprint of the scene as a low-resolution image"),
            (r"preview\/browse\.tif$", "thumbnail", "A thumbnail image of the scene"),
            (r"preview\/composite_ql\.tif$", "quicklook", "A composite quicklook image composed of all layers"),
            (r"preview\/[^\\:?\"<>|]+\.tif$", "quicklookLayer", "Individual quicklook layer"),
        ],
        "optical": [
            # Imagery
            (r"\/img_[^\\:?\"<>|]+_r\d+c\d+\.(tif|tiff|jp2)$", "primaryAsset", "Full resolution image file, possibly tiled. Row (R) and Col (C) image tile indexes"),
            (r"\/img_[^\\:?\"<>|]+_r\d+c\d+\.(tfw|j2w)$", "georeference", "Simple assembling/georeferencing file, possibly tiled. Row (R) and Col (C) image tile indexes"),
            (r"\/preview_[^\\:?\"<>|]+\.jpg$", "quicklook", "Quicklook raster file"),
            (r"\/preview_[^\\:?\"<>|]+\.kmz$", "quicklookKMZ", "Quicklook KMZ file"),
            (r"\/icon_[^\\:?\"<>|]+\.jpg$", "thumbnail", "Thumbnail raster file"),
            # Metadata
            (r"\/dim_[^\\:?\"<>|]+\.xml$", "DIMAP", "Main product metadata file"),
            (r"\/iso_[^\\:?\"<>|]+\.xml$", "ISO", "ISO 19115/19139 metadata file"),
            (r"\/lut_[^\\:?\"<>|]+\.xml$", "LUT", "DIMAP, LUT colour curves metadata file"),
            (r"\/rpc_[^\\:?\"<>|]+\.xml$", "RPC", "DIMAP, RPC metadata file"),
            (r"\/ground_[^\\:?\"<>|]+\.xml$", "GROUND", "DIMAP, Ground Source metadata file"),
            (r"\/height_[^\\:?\"<>|]+\.xml$", "HEIGHT", "DIMAP, Height Source metadata file"),
            (r"\/processing_[^\\:?\"<>|]+\.xml$", "PROCESSING", "DIMAP, Processing lineage file"),
            (r"\/gipp_[^\\:?\"<>|]+\.xml$", "GIPP", "Ground Image Processing Parameters file"),
            (r"\/strip_[^\\:?\"<>|]+\.xml$", "STRIP", "DIMAP, Data Strip Source metadata file"),
            # Masks
            (r"\/masks\/roi_[^\\:?\"<>|]+\.gml$", "ROIMask", "GML, Region of interest vector mask"),
            (r"\/masks\/cld_[^\\:?\"<>|]+\.gml$", "CLDMask", "GML, Cloud vector mask"),
            (r"\/masks\/qte_[^\\:?\"<>|]+\.gml$", "QTEMask", "GML, Synthetic technical quality vector mask"),
            (r"\/masks\/snw_[^\\:?\"<>|]+\.gml$", "SNWMask", "GML, Snow vector mask"),
            (r"\/masks\/det_[^\\:?\"<>|]+\.gml$", "DETMask", "GML, Out of order detectors vector mask"),
            (r"\/masks\/vis_[^\\:?\"<>|]+\.gml$", "VISMask", "GML, Hidden area vector mask"),
            (r"\/masks\/slt_[^\\:?\"<>|]+\.gml$", "SLTMask", "GML, Straylight vector mask"),
            (r"\/masks\/dtm_[^\\:?\"<>|]+\.gml$", "DTMMask", "GML, DTM quality vector mask"),
            (r"\/masks\/wat_[^\\:?\"<>|]+\.gml$", "WATMask", "GML, Water areas vector mask"),
            (r"\/masks\/cut_[^\\:?\"<>|]+\.shp$", "CUTMask", "Shapefile, cutline vector mask"),
            (r"\/masks\/ppm_[^\\:?\"<>|]+\.$", "PPMMask", "Planimetric accuracy Performance assessment Mask, raster"),
            # Miscellaneous
            (r"\/vol_[^\\:?\"<>|]+\.xml$", "indexVolume", "Index volume file of products contained in the delivery"),
            (r"\/delivery\.pdf$", "delivery", "Delivery note"),
            (r"\/license\.pdf$", "license", "License file"),
            (r"\/index\.htm$", "index", "Index file"),
            (r"logo\.jpg$", "logo", "Logo file"),
            (r"style\.xsl$", "styleSheet", "Short metadata content for discovering purpose"),
        ],
    }

    # Select the appropriate regex list based on collection_id
    patterns = regex_patterns.get(collection_id, regex_patterns["optical"])

    # Test the file path against each regex
    for pattern, name, description in patterns:
        if re.search(pattern, file_path.lower()):
            return name, description

    # If no match is found, return file name and empty description
    return os.path.basename(file_path), ""


def update_stac_item_failure(
    stac_item: dict,
    file_name: str,
    collection_id: str,
    reason: str,
    workspace: str,
    workspace_bucket: str,
    order_id: str = None,
) -> None:
    """Update the STAC item with the failure order status"""
    # Mark the order as failed in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.FAILED.value)

    # Mark the reason for the failure in the local STAC item
    stac_item["properties"]["order_failure_reason"] = reason

    # Update the 'updated' field to the current time
    current_time = current_time_iso8601()
    stac_item["properties"]["updated"] = current_time

    # Create local record of attempted order, to be used as the workflow output
    write_stac_item_and_catalog(
        stac_item, file_name, collection_id, order_id, workspace, workspace_bucket
    )


def update_stac_item_ordered(
    stac_item: dict,
    collection_id: str,
    item_id: str,
    order_id: str,
    s3_bucket: str,
    pulsar_url: str,
    workspace: str,
):
    """Update the STAC item with the ordered order status"""
    logging.info(f"Updating STAC item with order ID: {order_id} to 'ordered' status.")
    # Mark the order as ordered in the local STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.ORDERED.value)

    # Update the 'updated' field to the current time
    current_time = current_time_iso8601()
    stac_item["properties"]["updated"] = current_time
    stac_item["properties"]["order:date"] = current_time

    # Ingest the updated STAC item to the catalog
    try:
        ingest_stac_item(
            stac_item, s3_bucket, pulsar_url, workspace, collection_id, item_id
        )
    except Exception as e:
        logging.error(f"Failed to ingest STAC item: {e}", exc_info=True)


def update_stac_item_success(
    stac_item: dict,
    file_name: str,
    collection_id: str,
    order_id: str,
    directory: str,
    workspace: str,
    workspace_bucket: str,
):
    """Update the STAC item with the assets and success order status"""
    # Add all files in the directory as assets to the STAC item
    name_counter = {}
    for root, dirs, files in os.walk(directory):
        dirs.sort()
        for asset in sorted(files):
            asset_path = os.path.join(root, asset)
            asset_name, description = get_asset_details(asset_path, collection_id)

            # Append row and column indexes to the asset name if they exist
            match = re.search(r"_R\d+C\d+\.", asset_path.upper())
            if match:
                # Remove trailing dot originating from the extension
                asset_name = asset_name + match.group(0)[:-1]

            # Cannot have duplicate asset names
            if asset_name in name_counter:
                # Append an incrementing integer
                name_counter[asset_name] += 1
                asset_name = f"{asset_name}_{name_counter[asset_name]}"
            else:
                name_counter[asset_name] = 0

            # Determine the MIME type of the file
            mime_type, _ = mimetypes.guess_type(asset_path)
            if mime_type is None:
                mime_type = "application/octet-stream"  # Default MIME type

            # Add asset link to the file
            stac_item["assets"][asset_name] = {
                "href": asset_path,
                "type": mime_type,
            }
            if description:
                stac_item["assets"][asset_name]["title"] = description
    # Mark the order as succeeded and upload the updated STAC item
    update_stac_order_status(stac_item, order_id, OrderStatus.SUCCEEDED.value)

    # Update the 'updated' and 'published' fields to the current time
    current_time = current_time_iso8601()
    stac_item["properties"]["updated"] = current_time
    stac_item["properties"]["published"] = current_time

    # Create local record of the order, to be used as the workflow output
    write_stac_item_and_catalog(
        stac_item, file_name, collection_id, order_id, workspace, workspace_bucket
    )


def get_item_hrefs_from_catalogue(catalogue_dir: str) -> list:
    """Return a list of all hrefs to items in the STAC catalog"""
    catalog_path = os.path.join(catalogue_dir, "catalog.json")
    if not os.path.exists(catalog_path):
        raise FileNotFoundError(f"The file {catalog_path} does not exist.")

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    item_hrefs = []
    for link in catalog.get("links", []):
        if link.get("rel") == "item":
            href = link.get("href")
            absolute_href = os.path.normpath(os.path.join(catalogue_dir, href))
            item_hrefs.append(absolute_href)

    return item_hrefs


def is_valid_coordinate(coordinate: Coordinate) -> bool:
    """Check if a single coordinate is valid."""
    if not isinstance(coordinate, (list, tuple)) or len(coordinate) != 2:
        logging.warning(
            f"Invalid coordinate format: {coordinate}, type: {type(coordinate)}, length: {len(coordinate)}"
        )
        return False
    longitude, latitude = coordinate
    if not isinstance(latitude, (int, float)) or not isinstance(
        longitude, (int, float)
    ):
        logging.warning(
            f"Invalid coordinate type. {longitude}: {type(longitude)}, {latitude}: {type(latitude)}"
        )
        return False
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        logging.warning(
            f"Invalid coordinate value: longitude={longitude}, latitude={latitude}"
        )
        return False
    return True


def verify_coordinates(coordinates: List[List[Coordinate]]) -> bool:
    """Verify that a list of coordinates is valid."""
    return all(
        all(is_valid_coordinate(coord) for coord in polygons)
        for polygons in coordinates
    )
