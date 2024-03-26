import json
import os

import pendulum
from airflow.decorators import dag, task

# This is a way to create a different pipeline for every country, so they
# (hopefully) run in parallel.
configs = {
    "Afganistan": {"code": "afg"},
    "Mozambique": {"code": "moz"}
}

for config_name, config in configs.items():
    dag_id = f"dynamic_generated_dag_{config_name}"


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
            os.makedirs("data/input", exist_ok=True)
            os.makedirs(f"data/output/{config['code']}", exist_ok=True)
            os.makedirs(f"data/cmfs/{config['code']}", exist_ok=True)
            print("\\\\\\", os.listdir())
            print("\\\\\\", os.listdir("data/output"))

        @task()
        def download_hdx_admin_pop():
            import requests
            with open("dags/static_data/hdx_admin_pop_urls.json") as file:
                country_data = json.load(file)
                data = [x for x in country_data if x['country_code'] == config['code']][
                    0]
                print(data)
                response = requests.get(data['val'][0]['download_url'])
                with open(f"data/output/{config['code']}/hdx_pop_{config['code']}.csv", "wb") as output_file:
                    output_file.write(response.content)
                print(response.content)
                print(os.listdir(f"data/output/{config['code']}/"))

        @task()
        def elevation():
            pass

        @task()
        def worldpop1km():
            """ Download worldpop1km for country """
            pass

        @task()
        def mapaction_export():
            pass

        @task()
        def ocha_admin_boundaries():
            pass

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
