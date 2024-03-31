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
            from pipline_lib.make_data_dirs import make_data_dirs as make_dirs
            make_dirs(data_in_directory, data_out_directory, cmf_directory)

        @task()
        def download_hdx_admin_pop():
            from pipline_lib.download_hdx_admin_pop import \
                download_hdx_admin_pop as download_pop
            download_pop(country_code)

        @task()
        def elevation():
            pass

        @task()
        def worldpop1km():
            from pipline_lib.worldpop1km import worldpop1km as _worldpop1km
            _worldpop1km(country_code)

        @task()
        def mapaction_export():
            from pipline_lib.s3 import upload_to_s3, create_file

            create_file()
            upload_to_s3(s3_bucket=S3_BUCKET)

        @task()
        def ocha_admin_boundaries():
            from pipline_lib.ocha_admin_boundaries import \
                ocha_admin_boundaries as _ocha_admin_boundaries

            _ocha_admin_boundaries(country_code, data_in_directory, data_out_directory)

        @task()
        def healthsites():
            from pipline_lib.healthcities import healthsites
            healthsites()
            # TODO: not working, waiting for API key


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

        #####################################
        ######## Pipeline definition ########
        #####################################
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
