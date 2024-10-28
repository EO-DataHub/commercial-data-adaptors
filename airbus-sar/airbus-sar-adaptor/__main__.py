import requests
import boto3
import time
import json
import mimetypes
import base64
import sys
from kubernetes import client, config

s3 = boto3.client('s3')

# TODO: use airbus namespace
def get_api_key_from_secret(secret_name, secret_key, namespace='ws-apalmer-2dtpzuk'):
    # Load the Kubernetes configuration
    config.load_incluster_config()
    
    # Create a Kubernetes API client
    v1 = client.CoreV1Api()
    
    # Retrieve the secret
    secret = v1.read_namespaced_secret(secret_name, namespace)
    
    # Decode the secret
    api_key_base64 = secret.data[secret_key]
    api_key = base64.b64decode(api_key_base64).decode('utf-8')
    
    return api_key

def generate_access_token(env="dev"):

    api_key = get_api_key_from_secret("api-keys", "airbus-key")

    if env == "prod":
        url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
    else:
        url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = [
        ('apikey', api_key),
        ('grant_type', 'api_key'),
        ('client_id', 'IDP'),
    ]

    response = requests.post(url, headers=headers, data=data)

    return response.json()["access_token"]

def post_submit_order(acquisition_id, env="dev"):
    if env == "prod":
        url = "https://sar.api.oneatlas.airbus.com"
    else:
        url = "https://dev.sar.api.oneatlas.airbus.com"

    access_token = generate_access_token(env)    
    headers ={
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    body = {"acquisitions": [acquisition_id]}

    response = requests.post(f"{url}/v1/sar/orders/submit", json=body, headers=headers)
    # TODO: reinstate this
    # response.raise_for_status()

    body = response.json()
    print(f"Order submitted: {body}")
    # TODO: remove test item id
    return "test_item_id"
    for feature in body["features"]:
        if feature["properties"]["acquisitionId"] == acquisition_id:
            return feature["properties"]["itemId"]
    
    return None


def post_cancel_order(item_id, env="dev"):
    if env == "prod":
        url = "https://sar.api.oneatlas.airbus.com"
    else:
        url = "https://dev.sar.api.oneatlas.airbus.com"

    access_token = generate_access_token(env)    
    headers ={
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    body = {"items": [item_id]}

    response = requests.post(f"{url}/v1/sar/orders/cancel", json=body, headers=headers)
    response.raise_for_status()

    body = response.json()
    print(body)
    
    return None

def post_items_status(env="dev"):
    if env == "prod":
        url = "https://sar.api.oneatlas.airbus.com"
    else:
        url = "https://dev.sar.api.oneatlas.airbus.com"

    access_token = generate_access_token(env)    
    headers ={
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    body = {"limit": 200}

    response = requests.post(f"{url}/v1/sar/orders/*/items/status", json=body, headers=headers)
    response.raise_for_status()

    body = response.json()
    print(body)
    
    return None


def poll_s3_for_data(source_bucket: str, item_id: str, polling_interval: int = 60):
    """Poll the airbus S3 bucket for item_id and download the data"""    
    while True:
        # Check if the folder exists in the source bucket
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=f"{item_id}/")
        
        if 'Contents' in response:
            print(f"Folder '{item_id}' found in bucket '{source_bucket}'.")
            return response
        
        # Wait for the specified interval before checking again
        time.sleep(polling_interval)


def move_data_to_workspace(source_bucket: str, destination_bucket: str, parent_folder: str, response: dict):
    # Folder exists, move it to the destination bucket
    for obj in response['Contents']:
        copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
        destination_key = f"{parent_folder}/{obj['Key']}"
        
        # Copy object to the destination bucket
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        
        # Delete object from the source bucket
        s3.delete_object(Bucket=source_bucket, Key=obj['Key'])
        print(f"Moved object '{obj['Key']}' to '{destination_key}' in bucket '{destination_bucket}'.")

def retrieve_stac_item(bucket, key):
    # Retrieve the STAC item from S3
    stac_item_obj = s3.get_object(Bucket=bucket, Key=key)
    stac_item = json.loads(stac_item_obj['Body'].read().decode('utf-8'))
    return stac_item

def update_stac_item_success(bucket, key, parent_folder, item_id):
    stac_item = retrieve_stac_item(bucket, key)
    # List files in the specified folder
    folder_prefix = f"{parent_folder}/{item_id}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            asset_name = file_key.split('/')[-1]
            
            # Determine the MIME type of the file
            mime_type, _ = mimetypes.guess_type(file_key)
            if mime_type is None:
                mime_type = "application/octet-stream"  # Default MIME type

            # Add asset link to the STAC item
            stac_item['assets'][asset_name] = {
                "href": f"s3://{bucket}/{file_key}",
                "type": mime_type
            }
    update_stac_order_status(stac_item, item_id, "succeeded")
    upload_stac_item(bucket, key, stac_item)

def update_stac_item_failure(bucket, key, item_id):
    stac_item = retrieve_stac_item(bucket, key)
    update_stac_order_status(stac_item, item_id, "failed")
    upload_stac_item(bucket, key, stac_item)

def update_stac_order_status(stac_item, item_id, order_status):
    # Update or add fields relating to the order
    if 'properties' not in stac_item:
        stac_item['properties'] = {}
    
    if item_id is not None:
        stac_item['properties']['order.id'] = item_id
    stac_item['properties']['order.status'] = order_status

    # Update or add the STAC extension if not already present
    order_extension_url = "https://stac-extensions.github.io/order/v1.1.0/schema.json"
    if 'stac_extensions' not in stac_item:
        stac_item['stac_extensions'] = []
    
    if order_extension_url not in stac_item['stac_extensions']:
        stac_item['stac_extensions'].append(order_extension_url)

def upload_stac_item(bucket, key, stac_item):
    # Upload the modified STAC item back to S3
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(stac_item))
    print(f"Updated STAC item '{key}' in bucket '{bucket}' with new asset links.")


def get_acquisition_id_from_stac(bucket, key):
    stac_item = retrieve_stac_item(bucket, key)
    acquisition_id = stac_item.get('properties', {}).get('acquisition_id')
    if not acquisition_id:
        raise ValueError(f"Acquisition ID not found in STAC item '{key}'.")
    return acquisition_id

def main(stac_key: str, workspace_bucket: str):
    """Submit an order for an acquisition, wait for it to be processed and then retrieve the data"""
    # STAC should be generated before this, with an order status of ordered.
    stac_parent_folder = '/'.join(stac_key.split('/')[:-1])
    try:
        acquisition_id = get_acquisition_id_from_stac(workspace_bucket, stac_key)
        item_id = post_submit_order(acquisition_id)
    except Exception as e:
        print(f"Failed to submit order: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, None)
        return
    try:
        response = poll_s3_for_data("commercial-data-airbus", item_id)
        move_data_to_workspace("commercial-data-airbus", workspace_bucket, stac_parent_folder, response)
    except Exception as e:
        print(f"Failed to retrieve data: {e}")
        update_stac_item_failure(workspace_bucket, stac_key, item_id)
        return
    update_stac_item_success(workspace_bucket, stac_key, stac_parent_folder, item_id)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
