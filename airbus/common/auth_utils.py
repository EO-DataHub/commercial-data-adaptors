import base64
import json
import logging
import os

import boto3
import requests
from kubernetes import client, config


CLUSTER_PREFIX = os.getenv("CLUSTER_PREFIX", "eodhp")

def decrypt_airbus_api_key(ciphertext_b64: str, otp_key_b64: str) -> str:
    """
    Decrypts a ciphertext using One-Time Pad (OTP) via XOR.

    :param ciphertext_b64: Base64 encoded ciphertext from AWS Secrets Manager.
    :param otp_key_b64: Base64 encoded OTP key from Kubernetes Secret.
    :return: Decrypted plaintext API key.
    """

    try:
        # Decode both OTP key and ciphertext from Base64
        ciphertext = base64.b64decode(ciphertext_b64)
        otp_key = base64.b64decode(otp_key_b64)

        if len(ciphertext) != len(otp_key):
            raise ValueError("Ciphertext and OTP key must be the same length.")

        # XOR decryption
        plaintext_bytes = bytes(c ^ k for c, k in zip(ciphertext, otp_key))

        return plaintext_bytes.decode("utf-8")

    except UnicodeDecodeError:
        logging.error(
            "Warning: Decrypted data is not valid UTF-8. Returning raw bytes."
        )
        return plaintext_bytes.hex()
    except ValueError as e:
        logging.error(f"Integrity check failed: {e}")
        return None
    except Exception as e:
        logging.error(f"Decryption failed: {e}")
        return None


def get_airbus_api_key(workspace: str) -> str:
    """
    Retrieve an OTP (One-Time Pad) from Kubernetes Secrets and use it to decrypt
    an encrypted API key stored in AWS Secrets Manager.

    Steps:
    1. Load Kubernetes config and initialize the API client.
    2. Retrieve the OTP key from Kubernetes secret.
    3. Retrieve the ciphertext from AWS Secrets Manager.
    4. Use the OTP key to decrypt the ciphertext and return the plaintext API key.
    """

    provider = "airbus"

    # Initialize Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = f"ws-{workspace}"
    secretId = f"{namespace}-{CLUSTER_PREFIX}"

    # Retrieve the OTP key from Kubernetes Secrets
    logging.info("Fetching OTP key from Kubernetes...")
    secret_data = v1.read_namespaced_secret(f"otp-{provider}", namespace)
    otp_key_b64 = secret_data.data.get("otp")  # Adjusted key name for OTP

    if not otp_key_b64:
        raise ValueError(
            f"OTP key not found in Kubernetes Secret in namespace {namespace}."
        )

    # Initialize AWS Secrets Manager client and fetch the provider's ciphertext
    logging.info(
        f"Fetching ciphertext for provider '{provider}' from AWS Secrets Manager..."
    )
    secrets_client = boto3.client("secretsmanager")
    response = secrets_client.get_secret_value(SecretId=secretId)

    # Extract the secret string and parse it as JSON
    secret_string = response.get("SecretString", "{}")
    secret_dict = json.loads(secret_string)

    # Retrieve the encrypted API key (Base64 encoded ciphertext)
    ciphertext_b64 = secret_dict.get(provider)
    if not ciphertext_b64:
        raise ValueError(
            f"Ciphertext (encrypted API key) not found in AWS Secrets Manager for provider {provider}."
        )

    # Decrypt the API key using the OTP key
    plaintext_api_key = decrypt_airbus_api_key(ciphertext_b64, otp_key_b64)

    logging.info(f"Successfully fetched API key for {provider}")

    return plaintext_api_key


def get_airbus_contracts(workspace: str) -> str:
    """
    Retrieve the contracts for Airbus from K8s secret.

    """

    provider = "airbus"

    # Initialize Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = f"ws-{workspace}"

    # Retrieve the OTP key from Kubernetes Secrets
    logging.info("Fetching Contract IDs from Kubernetes...")
    secret_data = v1.read_namespaced_secret(f"otp-{provider}", namespace)
    contracts_b64 = secret_data.data.get("contracts") 

    if not contracts_b64:
        raise ValueError(f"Contracts not found in Kubernetes Secret in namespace {namespace}.")

    contracts = json.loads(base64.b64decode(contracts_b64).decode('utf-8')) 

    logging.info(f"Successfully fetched Contracts for {provider}")

    return contracts


def generate_access_token(workspace: str, env: str = "prod") -> str:
    """Generate an access token for the Airbus OneAtlas API"""

    api_key = get_airbus_api_key(workspace)
    if not api_key:
        raise ValueError("API key not found in secret")

    if env == "prod":
        url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
    else:
        url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = [
        ("apikey", api_key),
        ("grant_type", "api_key"),
        ("client_id", "IDP"),
    ]

    response = requests.post(url, headers=headers, data=data)
    return response.json()["access_token"]
