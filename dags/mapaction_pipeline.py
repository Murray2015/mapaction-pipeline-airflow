import json
import os

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
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
    docker_worker_working_dir = "/opt/airflow"


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
            print("////", data_in_directory, data_out_directory, cmf_directory, dag_id)
            make_dirs(data_in_directory, data_out_directory, cmf_directory)

        @task()
        def download_hdx_admin_pop():
            from pipline_lib.download_hdx_admin_pop import \
                download_hdx_admin_pop as download_pop

            print("////", data_in_directory, data_out_directory, cmf_directory)
            download_pop(country_code)

        @task()
        def elevation():
            pass

        @task()
        def worldpop1km():
            from pipline_lib.worldpop1km import worldpop1km as _worldpop1km
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _worldpop1km(country_code)

        @task()
        def mapaction_export():
            from pipline_lib.s3 import upload_to_s3, create_file

            print("////", data_in_directory, data_out_directory, cmf_directory)
            create_file()
            upload_to_s3(s3_bucket=S3_BUCKET)

        @task()
        def ocha_admin_boundaries():
            from pipline_lib.ocha_admin_boundaries import \
                ocha_admin_boundaries as _ocha_admin_boundaries

            print("////", data_in_directory, data_out_directory, cmf_directory)
            _ocha_admin_boundaries(country_code, data_in_directory, data_out_directory)

        @task()
        def healthsites():
            from pipline_lib.healthsities import healthsites as _healthsites
            print("////", data_in_directory, data_out_directory, cmf_directory)
            # _healthsites()
            # TODO: download working (but tiny (50) daily rate limit.
            # TODO: transform (extra by country) not done yet.

        @task()
        def ne_10m_roads():
            from pipline_lib.ne_10m_roads import ne_10m_roads as _ne_10m_roads
            print("////", data_in_directory, data_out_directory, cmf_directory)
            print(config)
            _ne_10m_roads(data_in_directory)

            # Download roads file
            # Unzip
            # extract from .shp

        @task()
        def ne_10m_populated_places():
            from pipline_lib.ne_10m_populated_places import ne_10m_populated_places as \
                _ne_10m_populated_places
            _ne_10m_populated_places()
            # TODO: extract from shapefile

        @task()
        def ne_10m_rivers_lake_centerlines():
            from pipline_lib.ne_10m_rivers_lake_centerlines import \
                ne_10m_rivers_lake_centerlines as _ne_10m_rivers_lake_centerlines
            _ne_10m_rivers_lake_centerlines(country_code, data_in_directory, data_out_directory)

        @task()
        def global_power_plant_database():
            from pipline_lib.power_plants import power_plants as _power_plants
            _power_plants(data_in_directory, data_out_directory)
            # TODO: extract via country code in csv in next step

        @task()
        def wfp_railroads():
            from pipline_lib.wfp_railroads import wfp_railroads as _wfp_railroads
            _wfp_railroads(data_in_directory, data_out_directory)
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

        test_bash_task = BashOperator(
            task_id="test_bash_task",
            # bash_command='echo {{ params.country_code }}',
            bash_command='pwd; ls /opt/airflow/dags/scripts;',
            params={"country_code": country_code}
        )

        # Make data/in dir
        ogrinfo_task = BashOperator(
            task_id="ogrinfo_task",
            bash_command='{{ params.bash_script_path }}/mapaction_extract_country_from_shp.sh {{ params.static_data_path }}{{ params.mask_path }} {{ params.docker_worker_working_dir }}/{{ params.data_path }} {{ params.output_path }}; ls /opt/airflow/data; ',
            params={
                "country_code": country_code,
                "mask_path": f"/countries/{country_code}.json",
                "data_path": f"{data_in_directory}/ne_10m_roads",
                "output_path": f"{data_out_directory}/ne_10m_roads",
                "docker_worker_working_dir": docker_worker_working_dir,
                "bash_script_path": f"{docker_worker_working_dir}/dags/scripts/bash",
                "static_data_path": f"{docker_worker_working_dir}/dags/static_data"
            }
        )

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
                 # ocha_admin_boundaries(),
                 mapaction_export(),
                 worldpop1km(),
                 elevation(),
                 download_hdx_admin_pop()]

                >>

                test_bash_task

                >>

                ogrinfo_task

                >>

                datasets_ckan_descriptions()

                >>

                [upload_datasets_all(), upload_cmf_all(), create_completeness_report()]

                >>

                send_slack_message()
        )


    mapaction_pipeline()
