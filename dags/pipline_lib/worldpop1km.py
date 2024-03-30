def worldpop1km(country_code: str):
    """ Download worldpop1km for country """

    import requests
    import os

    country_code_upper = country_code.upper()
    foldername = f"data/output/{country_code}/223_popu/"
    os.makedirs(foldername, exist_ok=True)

    with open(
            f"{foldername}/{country_code}_popu_pop_ras_s1_worldpop_pp_popdensity_2020unad.tif",
            "wb") as output_file:
        response = requests.get(
            f"https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/2020/{country_code_upper}/{country_code}_pd_2020_1km_UNadj.tif")
        output_file.write(response.content)
    print(os.listdir(foldername))

