# Proof of Concept - MapAction Airflow pipeline 

This is a proof of concept data pipeline using Apache Airflow to rebuild the 
[geocint mapaction pipeline](https://github.com/mapaction/geocint-mapaction/). 


## TODO 

- [ ] Test deploying to AWS / GCP 
- [ ] Connect to S3 / GCS 
- [ ] 


## Geocint <> Airflow POC mapping 

This table summarises the Geocint Makefile [recipie's prerequisites](https://github.com/mapaction/geocint-mapaction/blob/main/Makefile). This have been 
remade / mapped in the POC pipeline with the same names, and this is currently working
(although only 3 steps have actually been implemented.)

|   Stage name  |   Prerequisites   |   What it does / scripts it calls   |
|---|---|---|
|   Export country   |   NA - PHONY  |   Extracts polygon via osmium    Osm data import (import from ? To protocol buffer format)   Mapaction data table (upload to Postgres in new table)   Map action export (creates .shp and .json files for a list of ? [countries? Counties? Other?])   Mapaction upload cmf (uploads shp+tiff and geojson+tiff to s3, via ?cmf)  |
|   All   |   Dev   |     |
|   Dev  |   upload_datasets_all, upload_cmf_all, create_completeness_report  |   slack_message.py  |
|   .  |     |     |
|   upload_datasets_all   |   datasets_ckan_descriptions  |   mapaction_upload_dataset.sh  - Creates a folder and copies all .shp, .tif and .json files into it.   mapaction_upload_dataset.sh  - Zips it   Creates a folder called “/data/out/country_extractions/<country_name>” in S3, and copies the zip folder into it.   |
|   upload_cmf_all  |   cmf_metadata_list_all  |   See above by export country   |
|   datasets_ckan_descriptions  |   datasets_all  |   mapaction_build_dataset_description.sh -   |
|   datasets_all  |   ne_10m_lakes, ourairports, worldports, wfp_railroads, global_power_plant_database, ne_10m_rivers_lake_centerlines, ne_10m_populated_places, ne_10m_roads, healthsites, ocha_admin_boundaries, mapaction_export, worldpop1km, worldpop100m, elevation, download_hdx_admin_pop  |     |
|   ne_10m_lakes  |     |     |
|   ourairports  |     |     |
|   worldports  |     |     |
|   wfp_railroads  |     |     |
|   global_power_plant_database  |     |     |
|   ne_10m_rivers_lake_centerlines  |     |     |
|   ne_10m_populated_places  |     |     |
|   ne_10m_roads  |     |     |
|   Health sites  |     |     |
|   ocha_admin_boundaries  |     |     |
|   mapaction_export  |     |     |
|   worldpop1km  |     |     |
|   Elevation  |     |     |
|   download_hdx_admin_pop  |     |     |