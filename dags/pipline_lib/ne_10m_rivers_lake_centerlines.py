import io
import os
import zipfile
import requests


def ne_10m_rivers_lake_centerlines(country_code: str, data_in_dir: str,
                                   data_out_dir: str):
    # Make data/in dir
    save_dir = f"{data_in_dir}/ne_10m_rivers"
    print(f"save_dir: {save_dir}")
    os.makedirs(save_dir, exist_ok=True)

    response = requests.get(
        "https://naciscdn.org/naturalearth/10m/physical/ne_10m_rivers_lake_centerlines.zip")
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall(save_dir)
    print("////", os.listdir(save_dir))
