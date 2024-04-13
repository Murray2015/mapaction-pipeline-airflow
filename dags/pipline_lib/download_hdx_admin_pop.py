import json

from .utils import make_dir_download_file


def download_hdx_admin_pop(country_code: str, data_out_dir: str):
    with open("dags/static_data/hdx_admin_pop_urls.json") as file:
        country_data = json.load(file)
        data = [x for x in country_data if x['country_code'] == country_code][
            0]
        print(data)
        url = data['val'][0]['download_url']
        filename = data['val'][0]['name']
        make_dir_download_file(url, data_out_dir + "/hdx_admin_pop", filename)
