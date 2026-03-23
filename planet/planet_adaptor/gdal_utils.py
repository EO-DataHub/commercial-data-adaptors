import logging
from pathlib import Path

from osgeo import gdal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def gdal_translate(stac_item: dict) -> None:
    assets = stac_item["assets"]

    logging.info(assets)

    if not "primaryAsset" in assets:
        logging.error(f"No primary asset found for {stac_item}, skipping COG generation.")
        return

    input_file_name = Path(assets["primaryAsset"]["href"])
    input_id = stac_item["id"]
    output_file_name = input_file_name.parent / f"cog_{input_id}.tif"

    with (gdal.ExceptionMgr(), gdal.Open(input_file_name) as ds):
        sub_datasets = ds.GetSubDatasets()

        if sub_datasets:
            # Currently we only support one COG per feature, so just take the first one.
            ds_out = gdal.Translate(str(output_file_name), sub_datasets[0], format="COG", creationOptions=["BIGTIFF=YES"])
        else:
            ds_out = gdal.Translate(str(output_file_name), ds, format="COG", creationOptions=["BIGTIFF=YES"])

        ds_out = None
        ds = None

    cog_asset = {
        "href": str(output_file_name),
        "type": "image/tiff",
        "title": "COG of primaryAsset"
    }

    assets["cog"] = cog_asset
