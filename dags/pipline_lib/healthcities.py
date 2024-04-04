import requests


def healthsites(api_key: str):
    # Download healthsites
    response = requests.get(f"https://healthsites.io/api/v3/facilities/shapefile/World/download?api-key={api_key}")
    # Unzip file
    # Extra by country polygon (geojson)
    # TODO: My api key is now working, but shapefile download doesn't seem to work.
    # TODO: Looks like may need to use the list api and download as json. However, looks like
    # this doesn't take pagnination range, so need to loop through pages to download whole countries.
    pass
