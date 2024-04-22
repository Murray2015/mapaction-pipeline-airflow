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
# TODO: there's a bug in this config / the below for loop that isn't passing the 'code'
#  around correctly (is passing "moz" to both Afgan and Moz pipelines?)

S3_BUCKET = os.environ.get("S3_BUCKET")

for country_name, config in configs.items():
    dag_id = f"dynamic_generated_dag_{country_name}"
    country_code = config['code']
    data_in_directory = f"data/input/{country_code}"
    data_out_directory = f"data/output/{country_code}"
    cmf_directory = f"data/cmfs/{country_code}"
    docker_worker_working_dir = "/opt/airflow"
    bash_script_path = f"{docker_worker_working_dir}/dags/scripts/bash"
    country_geojson_filename = f"{docker_worker_working_dir}/dags/static_data/countries/{country_code}.json"


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
            """ Development complete """
            from pipline_lib.make_data_dirs import make_data_dirs as make_dirs
            print("////", data_in_directory, data_out_directory, cmf_directory, dag_id)
            make_dirs(data_in_directory, data_out_directory, cmf_directory)

        @task()
        def download_hdx_admin_pop():
            """ Development complete """
            from pipline_lib.download_hdx_admin_pop import \
                download_hdx_admin_pop as download_pop

            print("////", data_in_directory, data_out_directory, cmf_directory)
            download_pop(country_code, data_in_directory)

        @task()
        def elevation():
            from pipline_lib.srtm_30m import download_srtm_30
            # download_srtm_30("Mozambique")  # Commented for development as slow to run
            # TODO: this downloads ?30m SRTM, but also need to download 90 and 250m
            # TODO: not currently doing any processing here. May need to grid and output

        @task()
        def worldpop1km():
            """ Development complete """
            from pipline_lib.worldpop1km import worldpop1km as _worldpop1km
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _worldpop1km(country_code)

        @task()
        def worldpop100m():
            """ Development complete """
            from pipline_lib.worldpop100m import worldpop100m as _worldpop100m
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _worldpop100m(country_code)

        @task()
        def mapaction_export():
            from pipline_lib.s3 import upload_to_s3, create_file
            print("////", data_in_directory, data_out_directory, cmf_directory)
            create_file()
            upload_to_s3(s3_bucket=S3_BUCKET)

        @task()
        def ocha_admin_boundaries():
            """ Development complete """
            from pipline_lib.ocha_admin_boundaries import \
                ocha_admin_boundaries as _ocha_admin_boundaries

            print("////", data_in_directory, data_out_directory, cmf_directory)
            _ocha_admin_boundaries(country_code, data_in_directory, data_out_directory)

        @task()
        def healthsites():
            """ Development complete (extraction already done as API is by country) """
            from pipline_lib.healthsities import healthsites as _healthsites
            print("////", data_in_directory, data_out_directory, cmf_directory)
            save_location = data_in_directory + "/healthsites"
            os.makedirs(save_location, exist_ok=True)
            # _healthsites(country_name, os.environ.get("HEALTHSITES_API_KEY"), save_location)
            # Download working (but tiny (50) daily rate limit.

        @task()
        def ne_10m_roads():
            """ Development complete """
            from pipline_lib.ne_10m_roads import ne_10m_roads as _ne_10m_roads
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _ne_10m_roads(data_in_directory)

        @task.bash()
        def transform_ne_10m_roads() -> str:
            """ Usure if dev complete - outputs empty for Moz. """
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_roads/ne_10m_roads.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/232_tran/{country_code}_tran_rds_ln_s0_naturalearth_pp_roads"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"

        @task()
        def ne_10m_populated_places():
            """ Development complete """
            from pipline_lib.ne_10m_populated_places import ne_10m_populated_places as \
                _ne_10m_populated_places
            _ne_10m_populated_places(data_in_directory)
            # TODO: extract from shapefile

        @task.bash()
        def transform_ne_10m_populated_places() -> str:
            """ Development complete, but no output, so possible bugs """
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_populated_places/ne_10m_populated_places.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/229_stle/{country_code}_stle_stl_pt_s0_naturalearth_pp_maincities"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"

        @task()
        def ne_10m_rivers_lake_centerlines():
            """ Development complete """
            from pipline_lib.ne_10m_rivers_lake_centerlines import \
                ne_10m_rivers_lake_centerlines as _ne_10m_rivers_lake_centerlines
            _ne_10m_rivers_lake_centerlines(country_code, data_in_directory,
                                            data_out_directory)

        @task.bash()
        def transform_ne_10m_rivers_lake_centerlines() -> str:
            """ Development complete, but no features, so bgs?"""
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_lakes/ne_10m_lakes.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_riv_ln_s0_naturalearth_pp_rivers"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"


        @task()
        def power_plants():
            from pipline_lib.power_plants import power_plants as _power_plants
            _power_plants(data_in_directory, data_out_directory)
            # TODO: extract via country code in csv in next step

        @task()
        def wfp_railroads():
            from pipline_lib.wfp_railroads import wfp_railroads as _wfp_railroads
            _wfp_railroads(data_in_directory, data_out_directory)
            # TODO: haven't found any source for this file yet 🤷

        @task()
        def worldports():
            from pipline_lib.worldports import worldports as _world_ports
            _world_ports(data_in_directory, data_out_directory)

        @task()
        def ourairports():
            from pipline_lib.ourairports import ourairports as _ourairports
            _ourairports(data_in_directory, data_out_directory)

        @task()
        def ne_10m_lakes():
            from pipline_lib.ne_10m_lakes import ne_10m_lakes as _ne_10m_lakes
            _ne_10m_lakes(data_in_directory, data_out_directory)

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
        ne_10m_roads_inst = ne_10m_roads()
        ne_10m_populated_place_inst = ne_10m_populated_places()
        transform_ne_10m_roads_inst = transform_ne_10m_roads()
        transform_ne_10m_populated_places_inst = transform_ne_10m_populated_places()
        datasets_ckan_descriptions_inst = datasets_ckan_descriptions()
        ne_10m_rivers_lake_centerlines_inst = ne_10m_rivers_lake_centerlines()
        transform_ne_10m_rivers_lake_centerlines_inst = transform_ne_10m_rivers_lake_centerlines()

        (
                make_data_dirs()

                >>

                [ne_10m_lakes(),
                 ourairports(),
                 worldports(),
                 wfp_railroads(),
                 power_plants(),
                 ne_10m_rivers_lake_centerlines_inst,
                 ne_10m_populated_place_inst,
                 ne_10m_roads_inst,
                 healthsites(),
                 # ocha_admin_boundaries(),
                 mapaction_export(),
                 worldpop1km(),
                 worldpop100m(),
                 elevation(),
                 download_hdx_admin_pop()]

                >>

                test_bash_task

                >>

                ogrinfo_task

                >>

                datasets_ckan_descriptions_inst

                >>

                [upload_datasets_all(), upload_cmf_all(), create_completeness_report()]

                >>

                send_slack_message()
        )

        ne_10m_roads_inst >> transform_ne_10m_roads_inst
        ne_10m_populated_place_inst >> transform_ne_10m_populated_places_inst
        ne_10m_rivers_lake_centerlines_inst >> transform_ne_10m_rivers_lake_centerlines_inst
        [transform_ne_10m_roads_inst,
         transform_ne_10m_populated_places_inst,
         transform_ne_10m_rivers_lake_centerlines_inst] >> datasets_ckan_descriptions_inst


    mapaction_pipeline()
