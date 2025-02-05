import logging

import requests
from common.auth_utils import generate_access_token


def post_submit_order(
    acquisition_id: str, collection_id: str, coordinates: list, order_options: dict, item_uuids: list = None
) -> str:
    """Submit an order for an optical acquisition via POST request"""
    url = "https://order.api.oneatlas.airbus.com/api/v1/orders"

    spectral_processing = "bundle"
    if collection_id == "airbus_pneo_data":
        product_type = "PleiadesNeoArchiveMono"
        contract_id = "CTR24005241"
        item_uuids = item_uuids
        item_id = None
        if len(item_uuids) > 1:
            product_type = "PleiadesNeoArchiveMulti"
            spectral_processing = "full_bundle"
    elif collection_id == "airbus_phr_data":
        product_type = "PleiadesArchiveMono"
        contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
        item_uuids = None
        item_id = acquisition_id
    elif collection_id == "airbus_spot_data":
        product_type = "SPOTArchive1.5Mono"
        contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
        item_uuids = None
        item_id = acquisition_id
    else:
        raise ValueError(f"Collection {collection_id} not recognised")

    request_body = {
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
        "customerReference": "Polygon 1",
        "optionsPerProductType": [
            {
                "productTypeId": product_type,
                "options": [
                    {"key": "delivery_method", "value": "on_the_flow"},
                    {"key": "fullStrip", "value": "false"},
                    {"key": "image_format", "value": "dimap_geotiff"},
                    {"key": "licence", "value": "standard"},
                    {"key": "pixel_coding", "value": "12bits"},
                    {"key": "priority", "value": "standard"},
                    {"key": "processing_level", "value": "primary"},
                    {"key": "radiometric_processing", "value": order_options.get("radiometricProcessing")},
                    {"key": "spectral_processing", "value": spectral_processing},
                ],
            }
        ],
        "orderGroup": "",
        "delivery": {"type": "network"},
    }

    if item_uuids:
        data_source_ids = []
        for item_uuid in item_uuids:
            data_source_ids.append(
                {"catalogId": "PublicMOC", "catalogItemId": item_uuid}
            )
        request_body["items"][0]["dataSourceIds"] = data_source_ids

    if item_id:
        request_body["items"][0]["datastripIds"] = [item_id]

    logging.info(f"Sending POST request to submit an order with {request_body}")

    return "placeholder"

    access_token = generate_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=request_body, headers=headers)
    response.raise_for_status()

    body = response.json()
    logging.info(f"Order submitted: {body}")
    return body.get("quotationId")
