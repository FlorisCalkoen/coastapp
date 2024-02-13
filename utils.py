
import os

import dotenv
import fsspec
import geopandas as gpd
from shapely.geometry import box


def extract_spatial_extents(base_path, storage_options=None):
    """
    Extracts the spatial extents of GeoParquet files located at the given base path.
    
    Parameters:
    - base_path: A string representing the path to the directory containing GeoParquet files.
    - storage_options: Optional dictionary of storage options to pass to fsspec.
    
    Returns:
    - DataFrame with columns ['href', 'geometry'] where 'geometry' is the spatial extent.
    """
    fs = fsspec.filesystem('file' if '://' not in base_path else base_path.split('://')[0], **(storage_options or {}))
    
    # List all parquet files within the base directory
    parquet_files = [p for p in fs.glob(f"{base_path}/**/*.parquet")]
    
    extents = []
    
    crs = []
    for pq_file in parquet_files:
        # Adjust for fsspec's handling of paths
        with fs.open(pq_file) as f:
            gdf = gpd.read_parquet(f)
            
        extent = gdf.total_bounds  # Returns (minx, miny, maxx, maxy)
        qk = pq_file.split("/")[-2]
        bbox = box(extent[0], extent[1], extent[2], extent[3])
        extents.append({
            "geometry": bbox,
            "quadkey": qk.split("=")[-1].strip("qk"),
            "href": pq_file,
        })
        crs.append(gdf.crs.to_epsg())
    if len(set(crs)) != 1:
        raise ValueError("All GeoParquet files must have the same CRS.")
    return gpd.GeoDataFrame(extents, crs=crs[0])

# # Example usage for local storage
# href = "~/data/live/gcts-2000m.parquet"
# gdf = extract_spatial_extents(href)

dotenv.load_dotenv(override=True)

sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

href = "az://transects/gcts-2000m.parquet"
gdf = extract_spatial_extents(href, storage_options={'connection_string': azure_connection_string})

# aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
# aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# # Example usage for remote storage (e.g., S3)
# href = "s3://coastmonitor/gcts-2000m.parquet"
# gdf = extract_spatial_extents(href, storage_options={'key': aws_access_key_id, 'secret': aws_secret_access_key})

print("dpne")
