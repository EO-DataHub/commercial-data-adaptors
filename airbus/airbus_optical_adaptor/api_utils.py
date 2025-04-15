import logging
from datetime import datetime

import requests
from common.auth_utils import generate_access_token, get_airbus_contracts


def get_projection(
    contract_id: str, product_type: str, coordinates: list, workspace: str
) -> str:
    """Get the projection for the given coordinates"""
    url = f"https://order.api.oneatlas.airbus.com/api/v1/contracts/{contract_id}/productTypes/{product_type}/options"

    options_request_body = {
        "aoi": [
            {
                "polygonId": 1,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": coordinates,
                },
            }
        ]
    }
    access_token = generate_access_token(workspace)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=options_request_body, headers=headers)

    for option in response.json()["availableOptions"]:
        if option["name"] == "projection_1":
            return option["defaultValue"]
    return None


def build_order_request_body(
    acquisition_id: str,
    collection_id: str,
    coordinates: list,
    order_options: dict,
    workspace: str,
    licence: str,
    customer_reference: str,
    item_uuids: list = None,
    end_users: list = None,
) -> dict:
    """Get the body of the POST request to submit an order"""

    # Get the contract ID based on the collection ID
    contract_id = get_contract_id(workspace, collection_id)

    logging.info(f"Contract ID: {contract_id}")
    if not contract_id:
        raise ValueError(f"No contract ID found for collection {collection_id}")

    if collection_id == "airbus_pneo_data":
        product_type = "PleiadesNeoArchiveMono"
        item_uuids = item_uuids
        item_id = None
        if len(item_uuids) > 1:
            product_type = "PleiadesNeoArchiveMulti"
            order_options["spectralProcessing"] = "full_bundle"
        elif order_options.get("spectralProcessing") == "bundle":
            order_options["spectralProcessing"] = "full_bundle"
    elif collection_id == "airbus_phr_data":
        product_type = "PleiadesArchiveMono"
        item_uuids = None
        item_id = acquisition_id
    elif collection_id == "airbus_spot_data":
        product_type = "SPOTArchive1.5Mono"
        item_uuids = None
        item_id = acquisition_id
    else:
        raise ValueError(f"Collection {collection_id} not recognised")

    order_request_body = {
        "aoi": [
            {
                "id": 1,
                "name": "Polygon 1",
                "geometry": {"type": "Polygon", "coordinates": coordinates},
            }
        ],
        "programReference": "",
        "contractId": contract_id,
        "items": [
            {
                "notifications": [],
                "stations": [],
                "productTypeId": product_type,
                "aoiId": 1,
                "properties": [],
            }
        ],
        "primaryMarket": "NQUAL",
        "secondaryMarket": "",
        "customerReference": customer_reference,
        "optionsPerProductType": [
            {
                "productTypeId": product_type,
                "options": [
                    {"key": "delivery_method", "value": "on_the_flow"},
                    {"key": "fullStrip", "value": "false"},
                    {"key": "image_format", "value": "dimap_geotiff"},
                    {"key": "licence", "value": licence},
                    {"key": "pixel_coding", "value": order_options.get("pixelCoding")},
                    {"key": "priority", "value": "standard"},
                    {
                        "key": "processing_level",
                        "value": order_options.get("processingLevel"),
                    },
                    {
                        "key": "radiometric_processing",
                        "value": order_options.get("radiometricProcessing"),
                    },
                    {
                        "key": "spectral_processing",
                        "value": order_options.get("spectralProcessing"),
                    },
                ],
            }
        ],
        "orderGroup": "",
        "delivery": {"type": "network"},
    }

    if order_options.get("dem"):
        dem_key = "dem"
        if collection_id == "airbus_pneo_data":
            dem_key = "dem_1"
        order_request_body["optionsPerProductType"][0]["options"].append(
            {"key": dem_key, "value": order_options.get("dem")}
        )
    if order_options.get("projection"):
        projection = get_projection(contract_id, product_type, coordinates, workspace)
        order_request_body["optionsPerProductType"][0]["options"].append(
            {"key": "projection_1", "value": projection}
        )

    if item_uuids:
        data_source_ids = []
        for item_uuid in item_uuids:
            data_source_ids.append(
                {"catalogId": "PublicMOC", "catalogItemId": item_uuid}
            )
        order_request_body["items"][0]["dataSourceIds"] = data_source_ids

    if item_id:
        order_request_body["items"][0]["datastripIds"] = [item_id]

    if end_users:
        order_request_body["endUsers"] = end_users

    return order_request_body


def post_submit_order(
    acquisition_id: str,
    collection_id: str,
    coordinates: list,
    order_options: dict,
    workspace: str,
    licence: str,
    item_uuids: list = None,
    end_users: list = None,
) -> str:
    """Submit an order for an optical acquisition via POST request"""
    url = "https://order.api.oneatlas.airbus.com/api/v1/orders"

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    customer_reference = f"{workspace}_{timestamp}"

    request_body = build_order_request_body(
        acquisition_id,
        collection_id,
        coordinates,
        order_options,
        workspace,
        licence,
        customer_reference,
        item_uuids,
        end_users,
    )

    logging.info(f"Sending POST request to submit an order with {request_body}")

    access_token = generate_access_token(workspace)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=request_body, headers=headers)
    response.raise_for_status()

    body = response.json()
    logging.info(f"Order submitted: {body}")
    return body.get("salesOrderId"), customer_reference


def get_contract_id(workspace: str, collection_id: str) -> str:
    """Get the contract ID based on the workspace and collection ID"""

    contracts = get_airbus_contracts(workspace).get("optical", {})

    if collection_id == "airbus_pneo_data":
        for key, value in contracts.items():
            if "PNEO" in value:
                return key

    elif collection_id in ("airbus_phr_data", "airbus_spot_data"):
        for key, value in contracts.items():
            if "LEGACY" in value:
                return key
    return None
