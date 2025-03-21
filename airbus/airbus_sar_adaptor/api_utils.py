import logging

import requests
from common.auth_utils import generate_access_token


def post_submit_order(
    acquisition_id: str,
    order_options: dict,
    workspace: str,
    licence: str,
    env: str = "prod",
) -> str:
    """Submit an order for a SAR acquisition via POST request"""
    if env == "prod":
        url = "https://sar.api.oneatlas.airbus.com"
    else:
        url = "https://dev.sar.api.oneatlas.airbus.com"

    item_options = {
        "productType": order_options.get("productType"),
        "orbitType": order_options.get("orbit"),
        "gainAttenuation": 0,
    }
    if map_projection := order_options.get("mapProjection"):
        item_options["mapProjection"] = map_projection

    if resolution := order_options.get("resolution"):
        item_options["resolutionVariant"] = resolution

    body = {
        "acquisitions": [acquisition_id],
        "orderTemplate": licence,
        "orderOptions": item_options,
        "purpose": "IT Service Company",
    }

    logging.info(f"Sending POST request to submit an order with {body}")

    access_token = generate_access_token(workspace, env)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(f"{url}/v1/sar/orders/submit", json=body, headers=headers)
    response.raise_for_status()

    body = response.json()
    logging.info(f"Order submitted: {body}")
    for feature in body["features"]:
        if feature["properties"]["acquisitionId"] == acquisition_id:
            return feature["properties"]["orderItemId"]

    return None


def post_items_status(workspace: str, env: str = "prod") -> dict:
    """Query the status of all orders via POST request"""
    if env == "prod":
        url = "https://sar.api.oneatlas.airbus.com"
    else:
        url = "https://dev.sar.api.oneatlas.airbus.com"

    access_token = generate_access_token(workspace, env)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    body = {"limit": 200}

    logging.info(f"Sending POST request to query status of all orders with {body}")

    response = requests.post(
        f"{url}/v1/sar/orders/*/items/status", json=body, headers=headers
    )
    response.raise_for_status()

    body = response.json()
    logging.info(f"Status response: {body}")
    return body


def is_order_in_progress(
    acquisition_id: str, workspace: str, env: str = "prod"
) -> bool:
    """Check if an order for a SAR acquisition is in progress"""
    status = post_items_status(workspace, env)
    for feature in status:
        if feature.get("acquisitionId") == acquisition_id:
            return feature.get("status") == "submitted"

    return False
