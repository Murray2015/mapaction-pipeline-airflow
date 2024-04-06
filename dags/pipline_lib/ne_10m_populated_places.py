import os
import zipfile
from io import BytesIO

import requests


def ne_10m_populated_places(country_code: str, data_in_dir: str, data_out_dir: str):
    dir_name = "ne_10m_populated_places"
    # make in file
    os.makedirs(f"{data_in_dir}/{dir_name}")
    # download and unzip
    response = requests.get("https://naciscdn.org/naturalearth/10m/cultural/ne_10m_populated_places.zip")
    zf = zipfile.ZipFile(BytesIO(response.content))
    zf.extractall(f"{data_out_dir}/{dir_name}")
    # TODO: extract by country shape (in next BashOperator)


