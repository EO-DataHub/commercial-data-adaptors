import logging
from datetime import datetime

import requests
from common.auth_utils import generate_access_token


def get_projection(contract_id, product_type, coordinates):
    url = f"https://order.api.oneatlas.airbus.com/api/v1/contracts/{contract_id}/productTypes/{product_type}/options"

    body3 = {
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
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(url, json=body3, headers=headers)

    for option in response.json()["availableOptions"]:
        if option["name"] == "projection_1":
            return option["defaultValue"]


def get_body(collection: str, bundle_details: dict):
    if collection == "pneo":
        # # PNEO from catalog
        product_type = "PleiadesNeoArchiveMono"
        item_uuid = "3491fe8d-b604-492b-b65a-f8ebd88c5847"
        item_id = None
        contract_id = "CTR24005241"
        coordinates = [
            [
                [17.3299981635, -1.2748680949],
                [17.1991450348, -1.2757054871],
                [17.1990770161, -1.4086373468],
                [17.3298847906, -1.4080813917],
                [17.3299981635, -1.2748680949],
            ]
        ]
    elif collection == "spot":
        # SPOT
        product_type = "SPOTArchive1.5Mono"
        item_uuid = None
        item_id = "DS_SPOT6_202412170808264_FR1_FR1_SV1_SV1_E028N08_05768"
        contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
        coordinates = [
                    [
                        [
                            28.7707130729,
                            9.3177342568
                        ],
                        [
                            28.167841601,
                            9.2726100932
                        ],
                        [
                            28.1703098699,
                            8.63735883
                        ],
                        [
                            28.17031782,
                            8.6330773626
                        ],
                        [
                            28.1723280587,
                            7.9991214858
                        ],
                        [
                            28.172343762,
                            7.9948150065
                        ],
                        [
                            28.1741575314,
                            7.3579163056
                        ],
                        [
                            28.1741763453,
                            7.3536087883
                        ],
                        [
                            28.1745994828,
                            7.2772380279
                        ],
                        [
                            28.7667299886,
                            7.3151061973
                        ],
                        [
                            28.7665782564,
                            7.3917862323
                        ],
                        [
                            28.7665839839,
                            7.3961262385
                        ],
                        [
                            28.7676292546,
                            8.0357336182
                        ],
                        [
                            28.767634571,
                            8.0400546258
                        ],
                        [
                            28.7690283928,
                            8.6762187169
                        ],
                        [
                            28.7690416454,
                            8.6805080211
                        ],
                        [
                            28.7707130729,
                            9.3177342568
                        ]
                    ]
                ]
    elif collection == "phr":
        # # PHR
        product_type = "PleiadesArchiveMono"
        item_uuid = None
        item_id = "DS_PHR1A_202408062014568_FR1_PX_W131N61_0103_00596"
        contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
        coordinates = [
                    [
                        [
                            -130.9904204024051,
                            61.147710489765
                        ],
                        [
                            -131.0848619078786,
                            61.14650685178067
                        ],
                        [
                            -131.1592689394314,
                            61.14461918963631
                        ],
                        [
                            -131.1773758104632,
                            61.1444073926254
                        ],
                        [
                            -131.1771377359167,
                            61.08406753819755
                        ],
                        [
                            -131.1317005473507,
                            61.0844942111017
                        ],
                        [
                            -131.1131524796308,
                            61.08493645517736
                        ],
                        [
                            -131.075443467873,
                            61.08615782751919
                        ],
                        [
                            -131.0376176469411,
                            61.08693789392094
                        ],
                        [
                            -131.0000041023008,
                            61.08713166229637
                        ],
                        [
                            -130.9812119591603,
                            61.08698093743659
                        ],
                        [
                            -130.9625528320329,
                            61.08659792790926
                        ],
                        [
                            -130.9247397982764,
                            61.08707347622387
                        ],
                        [
                            -130.8504163434463,
                            61.08623153635694
                        ],
                        [
                            -130.8133079697347,
                            61.08668521148752
                        ],
                        [
                            -130.7951378228767,
                            61.08662527983136
                        ],
                        [
                            -130.7947154792121,
                            61.10102662138394
                        ],
                        [
                            -130.7945966002741,
                            61.11039822892862
                        ],
                        [
                            -130.7947077768797,
                            61.11943479278168
                        ],
                        [
                            -130.7945318981676,
                            61.12889579385388
                        ],
                        [
                            -130.794690481974,
                            61.13788806253959
                        ],
                        [
                            -130.7945708573638,
                            61.14731985891642
                        ],
                        [
                            -130.8224209744661,
                            61.14664486026957
                        ],
                        [
                            -130.8408170156885,
                            61.14669132008375
                        ],
                        [
                            -130.8962828290902,
                            61.14758892236727
                        ],
                        [
                            -130.915477347515,
                            61.14691267758437
                        ],
                        [
                            -130.9345577527323,
                            61.14649542824908
                        ],
                        [
                            -130.9530576731771,
                            61.14686368288686
                        ],
                        [
                            -130.971623630633,
                            61.14748915717179
                        ],
                        [
                            -130.9904204024051,
                            61.147710489765
                        ]
                    ]
                ]

    body2 = {
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
                    {"key": "pixel_coding", "value": bundle_details["pixelCoding"]},
                    {"key": "priority", "value": "standard"},
                    {"key": "processing_level", "value": bundle_details["processingLevel"]},
                    {"key": "radiometric_processing", "value": bundle_details["radiometricProcessing"]},
                    {"key": "spectral_processing", "value": bundle_details["spectralProcessing"]},
                ],
            }
        ],
        "orderGroup": "",
        "delivery": {"type": "network"},
    }

    if bundle_details.get("dem"):
        key = "dem"
        if collection == "pneo":
            key = "dem_1"
        body2["optionsPerProductType"][0]["options"].append({"key": key, "value": bundle_details["dem"]})
    if bundle_details.get("projection"):
        projection = get_projection(contract_id, product_type, coordinates)
        body2["optionsPerProductType"][0]["options"].append({"key": "projection_1", "value": projection})

    if item_uuid:
        body2["items"][0]["dataSourceIds"] = [{"catalogId": "PublicMOC", "catalogItemId": item_uuid}]
    if item_id:
        body2["items"][0]["datastripIds"] = [item_id]

    return body2


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

    if collection_id == "airbus_pneo_data":
        item_uuids = item_uuids
        item_id = None
    elif collection_id == "airbus_phr_data":
        item_uuids = None
        item_id = acquisition_id
    elif collection_id == "airbus_spot_data":
        item_uuids = None
        item_id = acquisition_id
    else:
        raise ValueError(f"Collection {collection_id} not recognised")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    customer_reference = f"{workspace}_{timestamp}"

    request_body = get_body(collection_id, bundle_details=order_options)

    if item_uuids:
        data_source_ids = []
        for item_uuid in item_uuids:
            data_source_ids.append(
                {"catalogId": "PublicMOC", "catalogItemId": item_uuid}
            )
        request_body["items"][0]["dataSourceIds"] = data_source_ids

    if item_id:
        request_body["items"][0]["datastripIds"] = [item_id]

    if end_users:
        request_body["endUsers"] = end_users

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
