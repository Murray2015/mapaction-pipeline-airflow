def download_hdx_admin_pop(country_code: str):
    import json
    import requests
    import os

    with open("dags/static_data/hdx_admin_pop_urls.json") as file:
        country_data = json.load(file)
        data = [x for x in country_data if x['country_code'] == country_code][
            0]
        print(data)
        response = requests.get(data['val'][0]['download_url'])
        with open(f"data/output/{country_code}/hdx_pop_{country_code}.csv",
                  "wb") as output_file:
            output_file.write(response.content)
        print(response.content)
        print(os.listdir(f"data/output/{country_code}/"))
