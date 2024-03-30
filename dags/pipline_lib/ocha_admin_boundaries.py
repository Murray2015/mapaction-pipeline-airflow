import glob


def ocha_admin_boundaries(country_code, data_in_directory):
    """ Downloads the hdx admin boundaries. Based on download_hdx_admin_boundaries.sh.

    First, downloads the data list from https://data.humdata.org/api/3/action/package_show?id=cod-ab-$country_code
    Then iterates through object downloading and un-compressing each file.
    """
    import io
    import requests
    import os
    import zipfile

    save_dir = f"{data_in_directory}/ocha_admin_boundaries"
    os.makedirs(save_dir, exist_ok=True)

    datalist_url = f"https://data.humdata.org/api/3/action/package_show?id=cod-ab-{country_code}"
    datalist_json = requests.get(datalist_url).json()
    download_urls: list[str] = [result['download_url'] for result in
                                datalist_json['result']['resources']]
    print(download_urls)
    # TODO: can speedup with asyncio/threading if needed
    for url in download_urls:
        save_location = f"{save_dir}/{url.split('/')[-1]}"
        response = requests.get(url)
        if url.endswith('.zip'):
            save_location = save_location.replace(".zip", "")
            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall(save_location)
        else:
            with open(save_location, 'wb') as f:
                f.write(response.content)
    print("final files", os.listdir(data_in_directory))
    # TODO: next, continue with the script with ogr2ogr stuff.

    # Return early if no spatial boundaries (e.g. Afghanistan)
    if not any([url.endswith(".zip") for url in download_urls]):
        return

    # Process shapefiles
    for filename in glob.glob(f"{save_dir}/**/*adm0*"):
        print("glob_filename: ", filename)

