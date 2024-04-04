#!/bin/bash

set -e

country_geojson_filename=$(basename "$1")
country_code="${country_geojson_filename%%.*}"

input_shp_name=$2
output_name="${3//"{country_code}"/$country_code}"
output_shp_name="$output_name.shp"
output_geojson_name="$output_name.geojson"

mkdir -p "$(dirname $output_shp_name)"

# try to fasten clipping
# getting spatial extent from country_geojson_filename
spatial_extent=$(ogrinfo -so -al static_data/countries/$country_geojson_filename | grep "Extent: " | sed 's/Extent: //g' |sed 's/[\(\),-]//g')
ogr2ogr -spat $spatial_extent -clipsrc static_data/countries/$country_geojson_filename -skipfailures $output_geojson_name $input_shp_name

# if number of features = 0 then delete geojson and do not create .shp
res=$(ogrinfo -so -al $output_geojson_name | grep "Feature Count:" | sed 's/Feature Count: //g')
if [ $res -eq 0 ]; then
    echo "No features, exiting and cleaning up"
    rm -f $output_geojson_name
else
    echo "Outputting features"
    ogr2ogr -lco ENCODING=UTF8 -skipfailures $output_shp_name $output_geojson_name
fi