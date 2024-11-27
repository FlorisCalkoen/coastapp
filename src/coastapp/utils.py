import datetime
import re
import unicodedata
from typing import Literal

import fsspec
import geopandas as gpd
from shapely.geometry import LineString, Polygon, base, box

from coastapp.specification import TypologyTrainSample


def extract_spatial_extents(base_path, storage_options=None):
    """
    Extracts the spatial extents of GeoParquet files located at the given base path.

    Parameters:
    - base_path: A string representing the path to the directory containing GeoParquet files.
    - storage_options: Optional dictionary of storage options to pass to fsspec.

    Returns:
    - DataFrame with columns ['href', 'geometry'] where 'geometry' is the spatial extent.
    """
    fs = fsspec.filesystem(
        "file" if "://" not in base_path else base_path.split("://")[0],
        **(storage_options or {}),
    )

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
        extents.append(
            {
                "geometry": bbox,
                "quadkey": qk.split("=")[-1].strip("qk"),
                "href": pq_file,
            }
        )
        crs.append(gdf.crs.to_epsg())
    if len(set(crs)) != 1:
        msg = "All GeoParquet files must have the same CRS."
        raise ValueError(msg)

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


def _buffer_geometry(
    geom: base.BaseGeometry, src_crs: str | int, buffer_dist: float
) -> base.BaseGeometry:
    """
    Buffers a single geometry in its appropriate UTM projection and reprojects it back to the original CRS.

    Args:
        geom (shapely.geometry.base.BaseGeometry): The geometry to buffer.
        src_crs (Union[str, int]): The original CRS of the geometry.
        buffer_dist (float): The buffer distance in meters.

    Returns:
        base.BaseGeometry: The buffered geometry in the original CRS.
    """
    # Estimate the UTM CRS based on the geometry's location
    utm_crs = gpd.GeoSeries([geom], crs=src_crs).estimate_utm_crs()

    # Reproject the geometry to UTM, apply the buffer, and reproject back to the original CRS
    geom_utm = gpd.GeoSeries([geom], crs=src_crs).to_crs(utm_crs).iloc[0]
    buffered_utm = geom_utm.buffer(buffer_dist)
    buffered_geom = gpd.GeoSeries([buffered_utm], crs=utm_crs).to_crs(src_crs).iloc[0]

    return buffered_geom


def buffer_geometries_in_utm(
    geo_data: gpd.GeoSeries | gpd.GeoDataFrame, buffer_dist: float
) -> gpd.GeoSeries | gpd.GeoDataFrame:
    """
    Buffer all geometries in a GeoSeries or GeoDataFrame in their appropriate UTM projections and return
    the buffered geometries in the original CRS.

    Args:
        geo_data (Union[gpd.GeoSeries, gpd.GeoDataFrame]): Input GeoSeries or GeoDataFrame containing geometries.
        buffer_dist (float): Buffer distance in meters.

    Returns:
        Union[gpd.GeoSeries, gpd.GeoDataFrame]: Buffered geometries in the original CRS.
    """
    # Determine if the input is a GeoDataFrame or a GeoSeries
    is_geodataframe = isinstance(geo_data, gpd.GeoDataFrame)

    # Extract the geometry series from the GeoDataFrame, if necessary
    geom_series = geo_data.geometry if is_geodataframe else geo_data

    # Ensure the input data has a defined CRS
    if geom_series.crs is None:
        msg = "Input GeoSeries or GeoDataFrame must have a defined CRS."
        raise ValueError(msg)

    # Buffer each geometry using the UTM projection and return to original CRS
    buffered_geoms = geom_series.apply(
        lambda geom: _buffer_geometry(geom, geom_series.crs, buffer_dist)
    )

    # Return the modified GeoDataFrame or GeoSeries with the buffered geometries
    if is_geodataframe:
        geo_data = geo_data.assign(geometry=buffered_geoms)
        return geo_data
    else:
        return buffered_geoms


def format_str_for_storage(
    input_str: str | None, replace_with: Literal["_", "-"]
) -> str:
    """
    Remove special characters from a string, handle accents, and replace spaces with the chosen character.

    Args:
        input_str (str): The input string to sanitize.
        replace_with (Literal["_", "-"]): Character to replace spaces with ('_' or '-').

    Returns:
        str: Sanitized string with no special characters and spaces replaced by the specified character.
    """
    if input_str is None:
        return ""  # Handle None input by returning an empty string

    # Normalize the string to NFD (Normalization Form Decomposition) to break characters into base and accent parts
    formatted_str = unicodedata.normalize("NFD", input_str)
    # Remove accents by filtering out the combining diacritical marks
    formatted_str = "".join(
        char for char in formatted_str if unicodedata.category(char) != "Mn"
    )
    # Convert to lowercase
    formatted_str = formatted_str.lower()
    # Replace spaces with the specified character
    formatted_str = re.sub(r"\s+", replace_with, formatted_str)
    # Remove any remaining characters that are not alphanumeric or the replacement character
    formatted_str = re.sub(rf"[^\w{replace_with}]", "", formatted_str)
    return formatted_str


def format_str_for_display(input_str: str | None) -> str:
    """
    Convert a snake_case or hyphenated string into a more readable format
    with only the first letter of the sentence capitalized.

    Args:
        input_str (str): The input string in snake_case or hyphenated format.

    Returns:
        str: A readable string with spaces and only the first letter capitalized.
    """
    if input_str is None:
        return ""  # Handle None input by returning an empty string

    # Replace underscores or hyphens with spaces
    formatted_str = input_str.replace("_", " ").replace("-", " ")

    # Capitalize only the first letter of the entire string
    formatted_str = formatted_str.capitalize()

    return formatted_str


def name_typology_record(record: TypologyTrainSample) -> str:
    """
    Generates the filename based on user, transect_id, and the record's timestamp.
    """
    user = record.user
    transect_id = record.transect.transect_id
    timestamp = (
        record.datetime_created or datetime.datetime.now(datetime.UTC).isoformat()
    )

    # Ensure the timestamp is a datetime object, if it's a string convert it
    if isinstance(timestamp, str):
        timestamp = datetime.datetime.fromisoformat(timestamp)

    # Format the timestamp to match the format you want (ISO format, without special characters)
    formatted_timestamp = timestamp.strftime("%Y%m%dT%H%M%S")

    return f"{user}_{transect_id}_{formatted_timestamp}.json"
