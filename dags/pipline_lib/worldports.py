import os

import requests


def worldports(data_in_dir: str, data_out_dir: str):
    download_location = f"{data_in_dir}/worldports"
    os.makedirs(download_location, exist_ok=True)
    url = "https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv"
    response = requests.get(url)
    response.raise_for_status()
    with open(f"{download_location}/worldports.csv", "wb") as f:
        f.write(response.content)

    # TODO: need to now extract from csv. Most pipeline tasks are extracting to a shapefile
    # via the scripts/mapaction_extract_country_from_csv.sh script