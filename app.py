import os
from typing import Literal

import dotenv
import duckdb
import geopandas as gpd
import geoviews as gv
import geoviews.tile_sources as gvts
import holoviews as hv
import panel as pn
import param
import pyproj
import pystac_client
import shapely.geometry
from holoviews import streams
from shapely import wkt
from shapely.geometry import Point
from shapely.wkb import loads

from utils import create_offset_rectangle

dotenv.load_dotenv(override=True)

class SpatialQueryEngine:
    def __init__(self, stac_url: str, collection_id: str, storage_backend: Literal['azure', 'aws'] = 'azure'):
        """
        Initializes the SpatialQueryEngine with STAC collection details.

        Args:
            stac_url: URL to the STAC catalog.
            collection_id: ID of the collection within the STAC catalog.
            storage_backend: Specifies the storage backend to use. Defaults to 'azure'.
        """
        self.storage_backend = storage_backend
        self.con = duckdb.connect(database=":memory:", read_only=False)
        self.con.execute("INSTALL spatial;")
        self.con.execute("LOAD spatial;")
        self.configure_storage_backend()
        
        # Directly load quadtiles from the STAC collection
        self.quadtiles = self.load_quadtiles_from_stac(stac_url, collection_id)
        if len(self.quadtiles["proj:epsg"].unique()) > 1:
            raise ValueError("Multiple CRSs found in the STAC collection.")
        self.proj_epsg = self.quadtiles["proj:epsg"].unique().item()

        self.radius = 10000.0  # Max radius for nearest search

    def configure_storage_backend(self):
        if self.storage_backend == 'azure':
            self.con.execute("INSTALL azure;")
            self.con.execute("LOAD azure;")
            self.con.execute(f"SET azure_storage_connection_string = '{os.getenv('AZURE_STORAGE_CONNECTION_STRING')}';")
        elif self.storage_backend == 'aws':
            self.con.execute("INSTALL httpfs;")
            self.con.execute("LOAD httpfs;")
            self.con.execute("SET s3_region = 'eu-west-2';")
            self.con.execute(f"SET s3_access_key_id = '{os.getenv('AWS_ACCESS_KEY_ID')}';")
            self.con.execute(f"SET s3_secret_access_key = '{os.getenv('AWS_SECRET_ACCESS_KEY')}';")


    def load_quadtiles_from_stac(self, stac_url: str, collection_id: str) -> gpd.GeoDataFrame:
        """Fetches and processes a STAC collection to create a GeoDataFrame of quadtiles."""
        stac_client = pystac_client.Client.open(stac_url)
        collection = stac_client.get_child(collection_id)
        items = collection.get_all_items()
        quadtiles = gpd.GeoDataFrame(
            [self.extract_storage_partition(item) for item in items], crs="EPSG:4326"
        )
        return quadtiles

    @staticmethod
    def extract_storage_partition(stac_item) -> dict:
        """Extracts geometry and href from a STAC item."""
        return {
            "geometry": shapely.geometry.shape(stac_item.geometry),
            "href": stac_item.assets["data"].href,
            "proj:epsg": stac_item.properties["proj:epsg"],
        }
    def get_nearest_geometry(self, x, y):
        point = Point(x, y)
        point_gdf = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")
        href = gpd.sjoin(self.quadtiles, point_gdf, predicate="contains").href.iloc[0]
        point_wkt = point_gdf.to_crs(self.proj_epsg).geometry.to_wkt().iloc[0]
        # NOTE: for DuckDB queries a small hack that replaces az:// with azure://
        if self.storage_backend == "azure":
            href = href.replace("az://", "azure://")

        query = f"""
        SELECT *, ST_Distance(ST_GeomFromWKB(geometry), ST_GeomFromText('{point_wkt}')) AS distance
        FROM '{href}'
        WHERE ST_DWithin(ST_GeomFromWKB(geometry), ST_GeomFromText('{point_wkt}'), {self.radius})
        ORDER BY distance
        LIMIT 1;
        """

        transect = self.con.execute(query).fetchdf()
        transect["geometry"] = transect.geometry.map(lambda b: loads(bytes(b)))
        return gpd.GeoDataFrame(transect, crs=self.proj_epsg)


class SpatialQueryApp(param.Parameterized):
    transect_name = param.String(default=None, doc="Identifier for the selected transect")
    # Using GeoDataFrame to store the current geometry explicitly
    current_geometry = param.Parameter(default=None, precedence=-1)

    def __init__(self, spatial_engine, visualization_func, default_geometry):
        super().__init__()
        self.spatial_engine = spatial_engine
        self.visualization_func = visualization_func
        self.default_geometry = default_geometry
        self.setup_ui()
        self.current_geometry = default_geometry

    def setup_ui(self):
        self.title = pn.pane.Markdown("# Coastal Annotation Application")
        self.tiles = gvts.EsriImagery.opts(width=500, height=500)
        self.point_draw = gv.Points([]).opts(size=10, color="red", tools=["hover"])
        self.point_draw_stream = streams.PointDraw(source=self.point_draw, num_objects=1)
        self.point_draw_stream.add_subscriber(self.on_point_draw)
        # Initialize the view with the default geometry visualization
        self.transect_view = pn.pane.HoloViews(self.visualization_func(self.default_geometry) * self.tiles * self.point_draw)

    def on_point_draw(self, data):
        if data:
            x, y = data["x"][0], data["y"][0]
            geometry = self.spatial_engine.get_nearest_geometry(x, y)
            self.current_geometry = geometry  # Directly update current geometry
            self.update_view()

    @param.depends('current_geometry', watch=True)
    def update_view(self):
        # Visualization logic is now centralized and reacts to changes in current_geometry
        try:
            # Assuming visualization_func can handle both GeoDataFrame or None as input
            new_view = self.visualization_func(self.current_geometry)
        except Exception as e:
            print(f"Visualization failed due to {e}. Defaulting to default geometry.")
            new_view = self.visualization_func(self.default_geometry)
        self.transect_view.object = new_view * self.tiles * self.point_draw

    def view(self):
        return pn.Column(self.title, self.transect_view, sizing_mode='stretch_width')


def default_visualization(transect):
    
    polygon = gpd.GeoDataFrame(
    geometry=[
        create_offset_rectangle(
            transect.to_crs(transect.estimate_utm_crs()).geometry.item(),
            distance=200,
        )
    ],
    crs=transect.estimate_utm_crs(),
    )

    polygon_plot = gv.Polygons(polygon[["geometry"]].to_crs(4326)).opts(
        fill_alpha=0.1, fill_color="green", line_width=2
    )

    transect_plot = gv.Path(transect[["geometry"]].to_crs(4326)).opts(
        color="red", line_width=1, tools=["hover"], active_tools=["wheel_zoom"]
    )

    return polygon_plot * transect_plot



def prepare_default_geometry(data, crs):
    """
    Prepares a default geometry from a data dictionary and sets its CRS This should
    exactly match what is being returned from the spatial engine.
    """
    geom = wkt.loads(data['geometry'])
    gdf = gpd.GeoDataFrame([data], geometry=[geom], crs=pyproj.CRS.from_user_input(crs))
    return gdf

pn.extension()
hv.extension("bokeh")

quadtile_href = "https://coclico.blob.core.windows.net/public/quadtiles-gcts-2000m.parquet"
stac_href = "https://coclico.blob.core.windows.net/stac/v1/catalog.json"


default_geometry = {
    "tr_name": "cl33475tr00223848",
    "lon": 4.27815580368042,
    "lat": 52.11359405517578,
    "bearing": 313.57275390625,
    "utm_crs": 32631,
    "coastline_name": 33475,
    "geometry": "LINESTRING (4.28855455531973 52.10728388554343, 4.267753743098557 52.119904391779215)",
    "bbox": {
        "maxx": 4.28855455531973,
        "maxy": 52.119904391779215,
        "minx": 4.267753743098557,
        "miny": 52.10728388554343,
    },
    "quadkey": "120201102230",
    "isoCountryCodeAlpha2": "NL",
    "admin_level_1_name": "Nederland",
    "isoSubCountryCode": "NL-ZH",
    "admin_level_2_name": "Zuid-Holland",
    "bounding_quadkey": "1202021102203",
}


default_geometry = prepare_default_geometry(default_geometry, crs=4326).to_crs(4326)

spatial_engine = SpatialQueryEngine(stac_href, collection_id="gcts-2000m", storage_backend="azure")
app = SpatialQueryApp(spatial_engine, default_visualization, default_geometry)
pn.Column(app.view()).servable()
