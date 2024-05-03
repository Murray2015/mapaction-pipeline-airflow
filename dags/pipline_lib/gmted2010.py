import rasterio
import geopandas as gpd
from rasterio.mask import mask

from .utils import make_dir_download_zip


def gmted2010(data_in_dir: str, data_out_dir: str):
    """ Extracts the gmted2010 dataset from the country outline polygon """
    # Define input and output paths
    arcgrid_path = ""
    polygon_path = ""
    output_path = ""

    # Read the ArcGrid data
    data = rasterio.open(arcgrid_path)
    profile = data.profile

    # Read the polygon data as a GeoDataFrame
    polygon_gdf = gpd.read_file(polygon_path)

    # Clip the data using polygon geometries
    clipped_data, clipped_transform = mask(data, polygon_gdf.geometry.values, crop=True)

    profile.update(
        driver="GTiff",
        height=clipped_data.shape[1],
        width=clipped_data.shape[2],
        count=1,
        dtype=clipped_data.dtype,
        transform=clipped_transform,
    )

    # Save the clipped data as a GeoTIFF
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(clipped_data)
