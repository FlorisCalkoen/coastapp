import fsspec
import geopandas as gpd
from shapely.geometry import LineString, Polygon, box


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

def generate_offset_line(line: LineString, offset: float) -> LineString:
    """
    Generate an offset line from the original line at a specified distance using offset_curve method.

    Args:
        line (LineString): The original line from which the offset is generated.
        offset (float): The distance for the offset. Positive values offset to the left,
            and negative values offset to the right.

    Returns:
        LineString: The offset line generated from the original line.
    """
    return line.offset_curve(offset) if offset != 0 else line


def create_offset_rectangle(line: LineString, distance: float) -> Polygon:
    """
    Construct a rectangle polygon using the original line and an offset distance.

    Args:
        line (LineString): The original line around which the polygon is constructed.
        distance (float): The offset distance used to create the sides of the polygon.

    Returns:
        Polygon: The constructed rectangle-shaped polygon.
    """

    # Create the offset lines
    left_offset_line = generate_offset_line(line, distance)
    right_offset_line = generate_offset_line(line, -distance)

    # Retrieve end points
    left_start, left_end = left_offset_line.coords[:]
    right_start, right_end = right_offset_line.coords[:]

    # Construct the polygon using the end points
    polygon = Polygon([left_start, left_end, right_end, right_start])

    return polygon
