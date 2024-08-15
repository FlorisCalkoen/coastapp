import logging
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
import pystac_client
import shapely
from classification import ClassificationManager
from feature import FeatureManager
from holoviews import streams
from schema import ClassificationSchemaManager
from shapely import wkt
from shapely.geometry import Point
from shapely.wkb import loads
from users import UserManager
from utils import create_offset_rectangle

logger = logging.getLogger(__name__)

dotenv.load_dotenv(override=True)
sas_token = os.getenv("APPSETTING_GCTS_AZURE_STORAGE_SAS_TOKEN")
storage_options = {"account_name": "coclico", "sas_token": sas_token}


class SpatialQueryEngine:
    def __init__(
        self,
        stac_url: str,
        collection_id: str,
        storage_backend: Literal["azure", "aws"] = "azure",
        storage_options: dict | None = None,
    ):
        """
        Initializes the SpatialQueryEngine with STAC collection details.
        """
        if not storage_options:
            storage_options = {}

        self.storage_backend = storage_backend
        self.con = duckdb.connect(database=":memory:", read_only=False)
        self.con.execute("INSTALL spatial;")
        self.con.execute("LOAD spatial;")
        self.configure_storage_backend()

        self.quadtiles = self.load_quadtiles_from_stac(stac_url, collection_id)
        if len(self.quadtiles["proj:epsg"].unique()) > 1:
            raise ValueError("Multiple CRSs found in the STAC collection.")
        self.proj_epsg = self.quadtiles["proj:epsg"].unique().item()

        self.radius = 10000.0  # Max radius for nearest search

    def configure_storage_backend(self):
        if self.storage_backend == "azure":
            self.con.execute("INSTALL azure;")
            self.con.execute("LOAD azure;")
        elif self.storage_backend == "aws":
            self.con.execute("INSTALL httpfs;")
            self.con.execute("LOAD httpfs;")
            self.con.execute("SET s3_region = 'eu-west-2';")
            self.con.execute(
                f"SET s3_access_key_id = '{os.getenv('AWS_ACCESS_KEY_ID')}';"
            )
            self.con.execute(
                f"SET s3_secret_access_key = '{os.getenv('AWS_SECRET_ACCESS_KEY')}';"
            )

    def load_quadtiles_from_stac(
        self, stac_url: str, collection_id: str
    ) -> gpd.GeoDataFrame:
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

        # Note: Handling azure and aws URLs
        if self.storage_backend == "azure":
            href = (
                href.replace("az://", "https://coclico.blob.core.windows.net/")
                + f"?{sas_token}"
            )

        minx, miny, maxx, maxy = point_gdf.total_bounds

        query = f"""
        SELECT 
            transect_id, 
            bbox, 
            lon,
            lat,
            ST_AsWKB(ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:4326', 'EPSG:4326')) AS geometry, 
            ST_Distance(
                ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:4326', 'EPSG:3857'),
                ST_Transform(ST_GeomFromText('{point_wkt}'), 'EPSG:4326', 'EPSG:3857')
            ) AS distance
        FROM 
            read_parquet('{href}')
        WHERE
            bbox.xmin <= {maxx} AND
            bbox.ymin <= {maxy} AND
            bbox.xmax >= {minx} AND
            bbox.ymax >= {miny}
        ORDER BY 
            distance
        LIMIT 1;
        """

        transect = self.con.execute(query).fetchdf()
        transect["geometry"] = transect.geometry.map(lambda b: loads(bytes(b)))
        return gpd.GeoDataFrame(transect, crs=self.proj_epsg)


class SpatialQueryApp(param.Parameterized):
    current_transect = param.ClassSelector(
        class_=gpd.GeoDataFrame, doc="Current transect as a GeoDataFrame"
    )

    def __init__(self, spatial_engine, default_geometry):
        super().__init__()
        self.spatial_engine = spatial_engine
        self.view_initialized = False  # Track if the view is initialized

        # Initialize the tiles and point draw tools
        self.tiles = gvts.EsriImagery()
        self.point_draw = gv.Points([]).opts(
            size=10, color="red", tools=["hover"], responsive=True
        )

        # Set the default transect without triggering a view update
        self.set_transect(default_geometry, query_triggered=False, update=False)

        # Initialize the UI components (view initialized first)
        self.transect_view = self.initialize_view()

        # Mark the view as initialized
        self.view_initialized = True

        # Setup the UI after the transect and view are initialized
        self.setup_ui()

    def setup_ui(self):
        """Set up the dynamic visualization and point drawing tools."""
        self.point_draw_stream = streams.PointDraw(
            source=self.point_draw, num_objects=1
        )
        self.point_draw_stream.add_subscriber(self.on_point_draw)

    def initialize_view(self):
        """Initializes the HoloViews pane using the current transect."""
        return pn.pane.HoloViews(
            self.plot_transect(self.current_transect)
            * self.tiles
            * self.point_draw
        )

    def plot_transect(self, transect):
        """Plot the given transect with polygons and paths."""
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

    def prepare_transect(self, transect_data):
        """Converts transect data dictionary to GeoDataFrame."""
        geom = wkt.loads(transect_data.get("geometry"))
        transect_gdf = gpd.GeoDataFrame([transect_data], geometry=[geom], crs="EPSG:4326")
        return transect_gdf

    def set_transect(self, transect_data, query_triggered=False, update=True):
        """Sets the current transect and optionally updates the view."""
        # If transect_data is a dictionary, prepare it as a GeoDataFrame
        if isinstance(transect_data, dict):
            self.current_transect = self.prepare_transect(transect_data)
        else:
            self.current_transect = transect_data

        # Update the view only if explicitly allowed and after initialization
        if update and self.view_initialized:
            self.update_view()

    def update_view(self):
        """Updates the visualization based on the current transect data."""
        try:
            new_view = self.plot_transect(self.current_transect)
        except Exception as e:
            logger.exception(
                f"Visualization failed due to {e}. Reverting to default transect."
            )
            new_view = self.plot_transect(self.default_transect)

        self.transect_view.object = new_view * self.tiles * self.point_draw

    def on_point_draw(self, data):
        """Handle the point draw event and query the nearest geometry based on drawn points."""
        if data:
            x, y = data["Longitude"][0], data["Latitude"][0]
            self.query_and_set_transect(x, y)

    def query_and_set_transect(self, x, y):
        """Queries the nearest transect and updates the current transect."""
        try:
            geometry = self.spatial_engine.get_nearest_geometry(x, y)
            self.set_transect(geometry, query_triggered=True)
        except Exception as e:
            logger.exception(
                "Failed to query geometry. Reverting to default transect."
            )
            self.set_transect(self.default_transect, query_triggered=False)

    def get_selected_geometry(self):
        """Returns the currently selected transect's geometry and metadata."""
        if not self.current_transect.empty:
            return self.current_transect.iloc[0].to_dict()
        return {"transect_id": None, "lon": None, "lat": None}

    def view(self):
        """Returns the pane representing the current transect view."""
        return self.transect_view


# Initialize the core application logic
pn.extension()
hv.extension("bokeh")

stac_url = "https://coclico.blob.core.windows.net/stac/test/catalog.json"

default_geometry = {
    "transect_id": "cl33475tr00223848",
    "lon": 4.27815580368042,
    "lat": 52.11359405517578,
    "bearing": 313.57275390625,
    "utm_epsg": 32631,
    "geometry": "LINESTRING (4.28855455531973 52.10728388554343, 4.267753743098557 52.119904391779215)",
    "bbox": {
        "xmax": 4.28855455531973,
        "ymax": 52.119904391779215,
        "xmin": 4.267753743098557,
        "ymin": 52.10728388554343,
    },
    "quadkey": "120201102230",
    "country": "NL",
    "common_country_name": "Nederland",
    "common_region_name": "NL-ZH",
}

spatial_engine = SpatialQueryEngine(
    stac_url=stac_url,
    collection_id="gcts",
    storage_backend="azure",
    storage_options=storage_options,
)

spatial_query_app = SpatialQueryApp(
    spatial_engine, default_geometry
)

# Initialize managers with the new app implementation
user_manager = UserManager(
    storage_options=storage_options, container_name="typology", prefix="users"
)
classification_schema_manager = ClassificationSchemaManager(
    storage_options=storage_options, container_name="typology", prefix=""
)
classification_manager = ClassificationManager(
    storage_options=storage_options,
    container_name="typology",
    prefix="labels",
    user_manager=user_manager,
    classification_schema_manager=classification_schema_manager,
    spatial_query_app=spatial_query_app,
)

feature_manager = FeatureManager(spatial_query_app=spatial_query_app)


# Define the Panel template
app = pn.template.FastListTemplate(
    title="Coastal Typology Annotation Tool",
    sidebar=[
        user_manager.view(),
        classification_schema_manager.view(),
        classification_manager.view(),
        feature_manager.view(),
    ],
    main=[spatial_query_app.view()],
    accent_base_color="#007BFF",
    header_background="#007BFF",
)

app.servable().show()
