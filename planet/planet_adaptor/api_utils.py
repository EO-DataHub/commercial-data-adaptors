import base64
import logging

import requests
from kubernetes import client, config
import planet


def get_api_key_from_secret(
    secret_name: str, secret_key: str, namespace: str = "ws-planet"
) -> str:
    """Retrieve an API key from a Kubernetes secret"""
    # Create a Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    # Retrieve and decode the secret
    secret = v1.read_namespaced_secret(secret_name, namespace)
    api_key_base64 = secret.data[secret_key]
    api_key = base64.b64decode(api_key_base64).decode("utf-8")

    return api_key


# def generate_access_token(env: str = "dev") -> str:
#     """Generate an access token for the Planet API"""
#     api_key = get_api_key_from_secret("api-keys", "airbus-key")
#
#     print('URL goes here')
#     url = '3'
#     # if env == "prod":
#     #     url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
#     # else:
#     #     url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"
#
#     headers = {
#         "Content-Type": "application/x-www-form-urlencoded",
#     }
#
#     data = [
#         ("apikey", api_key),
#         ("grant_type", "api_key"),
#         ("client_id", "IDP"),
#     ]
#
#     response = requests.post(url, headers=headers, data=data)
#
#     return response.json()["access_token"]


def define_delivery(credentials: dict, bucket: str):
    return planet.order_request.amazon_s3(
        credentials['AccessKeyId'],
        credentials['SecretAccessKey'],
        bucket,
        'eu-west-2',
        path_prefix="planet"
    )


def create_order_request(item_id: str, collection_id: str, delivery: dict) -> str:
    """Create an order for Planet data"""

    print(',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,')
   #  aoi = {
   #     "type":
   #     "Polygon",
   #     "coordinates": [[[-117.558734, 45.229745], [-117.452447, 45.229745],
   #                      [-117.452447, 45.301865], [-117.558734, 45.301865],
   #                      [-117.558734, 45.229745]]]
   # }
    aoi = {
       "type":
       "Polygon",
       "coordinates": [[[25.4, 35.31], [25.39, 35.3],
                        [25.4, 35.29], [25.41, 35.3],
                        [25.4, 35.31]]]
   }
    # TODO: remove this later

    order = planet.order_request.build_request(
        name='order',
        products=[
            planet.order_request.product(item_ids=[item_id],
                                         product_bundle='analytic_udm2',
                                         item_type=collection_id)
        ],
        tools=[planet.order_request.clip_tool(aoi=aoi)],
        delivery=delivery
    )

    return order


async def submit_order(order_details) -> str:
    """Submit an order for Planet data"""

    auth = planet.Auth.from_env(variable_name="PLANET_API_KEY")
    async with planet.Session(auth=auth) as sess:

        # 'orders' is the service name for the Orders API.
        cl = sess.client('orders')

        order = await cl.create_order(order_details)

        print('1111111111111111')
        print(order)
        return order
    # print('URL goes here')
    # url = '3'
    # # if env == "prod":
    # #     url = "https://sar.api.oneatlas.airbus.com"
    # # else:
    # #     url = "https://dev.sar.api.oneatlas.airbus.com"
    #
    # access_token = generate_access_token(env)
    # headers = {
    #     "Authorization": f"Bearer {access_token}",
    #     "Content-Type": "application/json",
    # }
    #
    # body = {
    #     "acquisitions": [acquisition_id],
    #     "orderTemplate": "Single User License",
    #     "orderOptions": {
    #         "productType": "MGD",
    #         "resolutionVariant": "RE",
    #         "orbitType": "science",
    #         "mapProjection": "auto",
    #         "gainAttenuation": 0,
    #     },
    #     "purpose": "IT Service Company",
    # }
    #
    # logging.info(f"Sending POST request to submit an order with {body}")
    #
    # response = requests.post(f"{url}/v1/sar/orders/submit", json=body, headers=headers)
    # response.raise_for_status()
    #
    # body = response.json()
    # logging.info(f"Order submitted: {body}")
    # for feature in body["features"]:
    #     if feature["properties"]["acquisitionId"] == acquisition_id:
    #         return feature["properties"]["orderItemId"]
    #
    # return None


# def post_cancel_order(item_id: str, env: str = "dev"):
#     """Cancel an order for a SAR acquisition via POST request"""
#
#     print('URL goes here')
#     url = '3'
#     # if env == "prod":
#     #     url = "https://sar.api.oneatlas.airbus.com"
#     # else:
#     #     url = "https://dev.sar.api.oneatlas.airbus.com"
#
#     access_token = generate_access_token(env)
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json",
#     }
#
#     body = {"items": [item_id]}
#
#     logging.info(f"Sending POST request to cancel an order with {body}")
#
#     response = requests.post(f"{url}/v1/sar/orders/cancel", json=body, headers=headers)
#     response.raise_for_status()
#
#     body = response.json()
#     logging.info(f"Order canceled: {body}")


# def post_items_status(env: str = "dev") -> dict:
#     """Query the status of all orders via POST request"""
#
#     print('URL goes here')
#     url = '3'
#     # if env == "prod":
#     #     url = "https://sar.api.oneatlas.airbus.com"
#     # else:
#     #     url = "https://dev.sar.api.oneatlas.airbus.com"
#
#     access_token = generate_access_token(env)
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json",
#     }
#
#     body = {"limit": 200}
#
#     logging.info(f"Sending POST request to query status of all orders with {body}")
#
#     response = requests.post(
#         f"{url}/v1/sar/orders/*/items/status", json=body, headers=headers
#     )
#     response.raise_for_status()
#
#     body = response.json()
#     logging.info(f"Status response: {body}")
#     return body


def is_order_in_progress(order: dict) -> bool:
    """Check if an order for a SAR acquisition is in progress"""
    # status = post_items_status(env)
    if order.get('status') == "queued":
        return True
    # for feature in status:
    #     if feature.get("acquisitionId") == acquisition_id:
    #         return feature.get("status") == "submitted"

    return False


# def get_order_templates(env: str = "dev") -> dict:
#     """Retrieve all available order templates via GET request"""
#     print('URL goes here')
#     url = '3'
#     # if env == "prod":
#     #     url = "https://sar.api.oneatlas.airbus.com"
#     # else:
#     #     url = "https://dev.sar.api.oneatlas.airbus.com"
#
#     access_token = generate_access_token(env)
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json",
#     }
#
#     logging.info("Sending GET request to retrieve order templates")
#
#     response = requests.get(f"{url}/v1/sar/config/orderTemplates", headers=headers)
#     response.raise_for_status()
#
#     body = response.json()
#     logging.info(f"Order templates: {body}")
#     return body