import io
import os
import zipfile

import requests


def ne_10m_roads(data_in_dir: str):
    # Make data/in dir
    save_dir = f"{data_in_dir}/ne_10m_roads"
    print(f"save_dir: {save_dir}")
    os.makedirs(save_dir, exist_ok=True)

    # Download roads file
    response = requests.get("https://naciscdn.org/naturalearth/10m/cultural/ne_10m_roads.zip")
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall(save_dir)
    print("////", os.listdir(save_dir))

    # extract from .shp done in next task
