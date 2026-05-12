import os
from dataclasses import dataclass

import requests

CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
ORGANIZATION_ID = int(os.getenv("ORGANIZATION_ID", ""))

access_token: str | None = None

@dataclass
class ContractInfo:
    contract_id: int
    organisation_id: int


def get_access_token() -> str:
    """
    Obtains an access token using the client secret flow. For production we'll need to get some
    secrets from AWS and obtain a refresh token first.
    """
    global access_token

    if access_token is not None:
        return access_token

    url = "https://login.open-cosmos.com/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "audience": "https://beeapp.open-cosmos.com"
    }

    response = requests.post(url, auth=(CLIENT_ID, CLIENT_SECRET), data=data)
    response.raise_for_status()

    j = response.json()

    if "access_token" not in j:
        raise Exception("Failed to obtain access token")

    access_token = j["access_token"]

    if access_token is None:
        raise Exception("Failed to obtain access token")

    return access_token


def get_contract_info() -> ContractInfo:
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    r = requests.get(f"https://app.open-cosmos.com/api/data/v1/dpap/organisations/{ORGANIZATION_ID}/policies", headers=headers)
    r.raise_for_status()
    policies = r.json()["data"]

    for policy in policies:
        if policy["default_contract"]:
            contract_id = policy["contract_id"]
            return ContractInfo(contract_id=contract_id, organisation_id=ORGANIZATION_ID)

    # If we don't have a default contract, use the first one.
    contract_id = policies[0]["contract_id"]
    return ContractInfo(contract_id=contract_id, organisation_id=ORGANIZATION_ID)


if __name__ == "__main__":
    print(get_contract_info())
