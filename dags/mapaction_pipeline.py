import json
import os

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dotenv import load_dotenv

load_dotenv()

# This is a way to create a different pipeline for every country, so they
# (hopefully) run in parallel.
configs = {
    "Afganistan": {"code": "afg"},
    "Mozambique": {"code": "moz"}
}

S3_BUCKET = os.environ.get("S3_BUCKET")

def create_file():
    with open('test.txt', 'w') as f:
        f.write('hello world')


def upload_to_s3() -> None:
    print("s3 bucket:", S3_BUCKET)
    hook = S3Hook('aws_conn')
    hook.load_file(
        filename="./test.txt",
        key="some_key/test.txt",
        bucket_name=S3_BUCKET,
        replace=True
    )


for config_name, config in configs.items():
    dag_id = f"dynamic_generated_dag_{config_name}"
    country_code = config['code']
    data_in_directory = f"data/input/{country_code}"
    data_out_directory = f"data/output/{country_code}"
    cmf_directory = f"data/cmfs/{country_code}"

    @dag(
        dag_id=dag_id,
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["mapaction"],
    )
    def mapaction_pipeline():
        """The MapAction Pipeline

        This pipeline extracts geospatial data from a variety of sources for analysis
        in an emergency.
        """

        @task()
        def make_data_dirs():
            print("This is //////// ", os.getcwd())
            os.makedirs(data_in_directory, exist_ok=True)
            os.makedirs(data_out_directory, exist_ok=True)
            os.makedirs(cmf_directory, exist_ok=True)
            print("\\\\\\", os.listdir())
            print("\\\\\\", os.listdir("data/input/"))
            print("\\\\\\", os.listdir("data/output"))

        @task()
        def download_hdx_admin_pop():
            import requests
            with open("dags/static_data/hdx_admin_pop_urls.json") as file:
                country_data = json.load(file)
                data = [x for x in country_data if x['country_code'] == country_code][
                    0]
                print(data)
                response = requests.get(data['val'][0]['download_url'])
                with open(f"data/output/{country_code}/hdx_pop_{country_code}.csv", "wb") as output_file:
                    output_file.write(response.content)
                print(response.content)
                print(os.listdir(f"data/output/{country_code}/"))

        @task()
        def elevation():
            pass

        @task()
        def worldpop1km():
            """ Download worldpop1km for country """

            import requests

            country_code_upper = country_code.upper()
            foldername = f"data/output/{country_code}/223_popu/"
            os.makedirs(foldername, exist_ok=True)

            with open(f"{foldername}/{country_code}_popu_pop_ras_s1_worldpop_pp_popdensity_2020unad.tif", "wb") as output_file:
                response = requests.get(f"https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/2020/{country_code_upper}/{country_code}_pd_2020_1km_UNadj.tif")
                output_file.write(response.content)
            print(os.listdir(foldername))


        @task()
        def mapaction_export():
            create_file()
            print(os.listdir())
            upload_to_s3()

        @task()
        def ocha_admin_boundaries():
            """ Downloads the hdx admin boundaries. Based on download_hdx_admin_boundaries.sh.

            First, downloads the data list from https://data.humdata.org/api/3/action/package_show?id=cod-ab-$country_code
            Then iterates through object downloading and un-compressing each file.
            """
            import io
            import requests
            import zipfile

            datalist_url = f"https://data.humdata.org/api/3/action/package_show?id=cod-ab-{country_code}"
            datalist_json = requests.get(datalist_url).json()
            download_urls: list[str] = [result['download_url'] for result in datalist_json['result']['resources']]
            print(download_urls)
            # TODO: can speedup with asyncio/threading if needed
            for url in download_urls:
                save_location = f"{data_in_directory}/{url.split('/')[-1]}"
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




        @task()
        def healthsites():
            pass

        @task()
        def ne_10m_roads():
            pass

        @task()
        def ne_10m_populated_places():
            pass

        @task()
        def ne_10m_rivers_lake_centerlines():
            pass

        @task()
        def global_power_plant_database():
            pass

        @task()
        def wfp_railroads():
            pass

        @task()
        def worldports():
            pass

        @task()
        def ourairports():
            pass

        @task()
        def ne_10m_lakes():
            pass

        @task()
        def datasets_ckan_descriptions():
            pass

        @task()
        def cmf_metadata_list_all():
            pass

        @task()
        def upload_cmf_all():
            pass

        @task()
        def upload_datasets_all():
            pass

        @task()
        def create_completeness_report():
            pass

        @task()
        def send_slack_message():
            pass

        (
                make_data_dirs()

                >>

                [ne_10m_lakes(),
                 ourairports(),
                 worldports(),
                 wfp_railroads(),
                 global_power_plant_database(),
                 ne_10m_rivers_lake_centerlines(),
                 ne_10m_populated_places(),
                 ne_10m_roads(),
                 healthsites(),
                 ocha_admin_boundaries(),
                 mapaction_export(),
                 worldpop1km(),
                 elevation(),
                 download_hdx_admin_pop()]

                >>

                datasets_ckan_descriptions()

                >>

                [upload_datasets_all(), upload_cmf_all(), create_completeness_report()]

                >>

                send_slack_message()
        )


    mapaction_pipeline()
