import logging
import os
from typing import Literal

import dotenv
import duckdb
import geopandas as gpd
import geoviews as gv
import geoviews.tile_sources as gvts
import panel as pn
import param
import pystac_client
import shapely
from holoviews import streams
from shapely import wkt
from shapely.geometry import Point
from shapely.wkb import loads

from coastapp.utils import buffer_geometries_in_utm, create_offset_rectangle

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
        self.stac_client = pystac_client.Client.open(stac_url)
        self.gcts_collection = self.stac_client.get_child(collection_id)
        items = self.gcts_collection.get_all_items()
        self.quadtiles = gpd.GeoDataFrame(
            [self.extract_storage_partition(item) for item in items], crs="EPSG:4326"
        )
        return self.quadtiles

    @staticmethod
    def extract_storage_partition(stac_item) -> dict:
        """Extracts geometry and href from a STAC item."""
        return {
            "geometry": shapely.geometry.shape(stac_item.geometry),
            "href": stac_item.assets["data"].href,
            "proj:epsg": stac_item.properties["proj:epsg"],
        }

    def get_random_transect(self):
        """
        Query one random transect from the parquet files with optional filtering by continent and country.
        """
        # Choose the href for remote parquet partition
        hrefs = self.quadtiles.href.unique().tolist()

        # Sign each HREF with the SAS token if the storage backend is Azure
        if self.storage_backend == "azure":
            signed_hrefs = []
            for href in hrefs:
                signed_href = href.replace(
                    "az://", "https://coclico.blob.core.windows.net/"
                )
                signed_href = signed_href + f"?{sas_token}"
                signed_hrefs.append(signed_href)
        else:
            signed_hrefs = hrefs

        # Join the hrefs into a single string
        hrefs_str = ", ".join(f'"{href}"' for href in signed_hrefs)

        # SQL query to randomly fetch one transect with filters
        # SQL query to fetch one random transect, ensuring filtering happens before sampling
        query = f"""
        WITH filtered_transects AS (
            SELECT
                transect_id,
                lon,
                lat,
                bbox,
                continent,
                country,
                ST_AsWKB(ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:4326', 'EPSG:4326')) AS geometry
            FROM read_parquet([{hrefs_str}])
            WHERE
                continent = 'EU'  -- Filter by continent
                AND country != 'RU'  -- Exclude records from Russia
        )
        SELECT *
        FROM filtered_transects
        USING SAMPLE 1 ROWS;
        """

        # Execute the query and fetch the result
        transect = self.con.execute(query).fetchdf()

        # Convert the geometry from WKB to GeoDataFrame format
        transect["geometry"] = transect.geometry.map(lambda b: loads(bytes(b)))

        # Return as GeoDataFrame with EPSG:4326 CRS
        return gpd.GeoDataFrame(transect, crs="EPSG:4326")

    def get_nearest_geometry(self, x, y):
        point = Point(x, y)
        point_gdf = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")
        href = gpd.sjoin(self.quadtiles, point_gdf, predicate="contains").href.iloc[0]
        area_of_interest = buffer_geometries_in_utm(point_gdf, self.radius)
        point_gdf_wkt = point_gdf.to_crs(self.proj_epsg).geometry.to_wkt().iloc[0]

        # Note: Handling azure and aws URLs
        if self.storage_backend == "azure":
            href = (
                href.replace("az://", "https://coclico.blob.core.windows.net/")
                + f"?{sas_token}"
            )

        minx, miny, maxx, maxy = area_of_interest.total_bounds

        query = f"""
        SELECT
            transect_id,
            bbox,
            lon,
            lat,
            ST_AsWKB(ST_Transform(geometry, 'EPSG:4326', 'EPSG:4326')) AS geometry,  -- Retrieve transect geometry as WKB
            ST_Distance(
                ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:3857', always_xy := true),  -- Transect origin in UTM
                ST_Transform(ST_GeomFromText('{point_gdf_wkt}'), 'EPSG:4326', 'EPSG:3857', always_xy := true)  -- Input point in UTM
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
        transect["geometry"] = transect.geometry.map(
            lambda b: loads(bytes(b))
        )  # Convert WKB to geometry
        return gpd.GeoDataFrame(
            transect, crs="EPSG:4326"
        )  # Return GeoDataFrame with transect geometry


class SpatialQueryApp(param.Parameterized):
    current_transect = param.ClassSelector(
        class_=gpd.GeoDataFrame, doc="Current transect as a GeoDataFrame"
    )
    show_labelled_transects = param.Boolean(
        default=False, doc="Show/Hide Labelled Transects"
    )

    def __init__(self, spatial_engine, labelled_transect_manager, default_geometry):
        super().__init__()
        self.spatial_engine = spatial_engine
        self.labelled_transect_manager = labelled_transect_manager
        self.view_initialized = False

        # Initialize map tiles and point drawing tools
        self.tiles = gvts.EsriImagery()
        self.point_draw = gv.Points([]).opts(
            size=10, color="red", tools=["hover"], responsive=True
        )

        # Set the default transect without triggering a view update
        self.default_geometry = default_geometry
        self.set_transect(self.default_geometry, query_triggered=False, update=False)

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

        # Add the toggle button to show/hide labelled transects
        self.toggle_button = pn.widgets.Toggle(
            name="Show Labelled Transects", value=False, button_type="default"
        )
        self.toggle_button.param.watch(self.toggle_labelled_transects, "value")

        self.get_random_transect_button = pn.widgets.Button(
            name="Get random transect (slow)", button_type="default"
        )
        self.get_random_transect_button.on_click(self._get_random_transect)

        # Add a radio button group for basemap selection
        self.basemap_button = pn.widgets.RadioButtonGroup(
            name="Basemap", options=["Esri Imagery", "OSM"], value="Esri Imagery"
        )
        self.basemap_button.param.watch(self.update_basemap, "value")

    def initialize_view(self):
        """Initializes the HoloViews pane using the current transect."""
        return pn.pane.HoloViews(
            (
                self.plot_transect(self.current_transect) * self.tiles * self.point_draw
            ).opts(active_tools=["wheel_zoom"])
        )

    def update_basemap(self, event):
        """Update the tiles based on the selected basemap."""
        if event.new == "Esri Imagery":
            self.tiles = gvts.EsriImagery()
        elif event.new == "OSM":
            self.tiles = gvts.OSM()

        # Update the view to reflect the new tiles
        self.update_view()

    def plot_transect(self, transect):
        """Plot the given transect with polygons and paths."""
        coords = list(transect.geometry.item().coords)
        landward_point, seaward_point = coords[0], coords[-1]
        # NOTE: I don't think showing the origin point is necessary
        # transect_origin_point = shapely.Point(transect.lon.item(), transect.lat.item())
        polygon = gpd.GeoDataFrame(
            geometry=[
                create_offset_rectangle(
                    transect.to_crs(transect.estimate_utm_crs()).geometry.item(), 200
                )
            ],
            crs=transect.estimate_utm_crs(),
        )
        polygon_plot = gv.Polygons(
            polygon[["geometry"]].to_crs(4326), label="Area of Interest"
        ).opts(fill_alpha=0.1, fill_color="green", line_width=2)
        transect_plot = gv.Path(
            transect[["geometry"]].to_crs(4326), label="Transect"
        ).opts(color="red", line_width=1, tools=["hover"])
        landward_point_plot = gv.Points([landward_point], label="Landward").opts(
            color="green", line_color="red", size=10
        )
        seaward_point_plot = gv.Points([seaward_point], label="Seaward").opts(
            color="blue", line_color="red", size=10
        )
        # NOTE: I don't think showing the origin point is necessary
        # transect_origin_point_plot = gv.Points(
        #     [transect_origin_point], label="Origin"
        # ).opts(color="red", line_color="red", size=10)

        return (
            polygon_plot * transect_plot * landward_point_plot * seaward_point_plot
        ).opts(legend_position="bottom_right")

    def prepare_transect(self, transect_data):
        """Converts transect data dictionary to GeoDataFrame."""
        geom = wkt.loads(transect_data.get("geometry"))
        transect_gdf = gpd.GeoDataFrame(
            [transect_data], geometry=[geom], crs="EPSG:4326"
        )
        return transect_gdf

    def set_transect(self, transect_data, query_triggered=False, update=True):
        """Sets the current transect and optionally updates the view."""
        if isinstance(transect_data, dict):
            self.current_transect = self.prepare_transect(transect_data)
        else:
            self.current_transect = transect_data

        # Update the view only if explicitly allowed and after initialization
        if update and self.view_initialized:
            self.update_view()

    def _get_random_transect(self, event):
        """Handle the button click to get a random transect."""
        transect = self.spatial_engine.get_random_transect()
        self.set_transect(transect, query_triggered=False)

    def toggle_labelled_transects(self, event):
        """Handle the toggle button to show or hide labelled transects."""
        self.show_labelled_transects = event.new

        if self.show_labelled_transects:
            self.toggle_button.button_type = "success"  # Set to green
            self.labelled_transect_manager.load()
        else:
            self.toggle_button.button_type = "default"
        self.update_view()

    def update_view(self):
        """Update the visualization based on the current transect."""
        new_view = self.plot_transect(self.current_transect)

        # If show_labelled_transects is True, include labelled transects in the view
        if self.show_labelled_transects:
            labelled_transects_plot = (
                self.labelled_transect_manager.plot_labelled_transects()
            )
            new_view = new_view * labelled_transects_plot

        self.transect_view.object = (new_view * self.tiles * self.point_draw).opts(
            legend_position="bottom_right", active_tools=["wheel_zoom"]
        )

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
        except Exception:
            logger.exception("Failed to query geometry. Reverting to default transect.")
            self.set_transect(self.default_geometry, query_triggered=False)

    def get_selected_geometry(self):
        """Returns the currently selected transect's geometry and metadata."""
        if not self.current_transect.empty:
            return self.current_transect.iloc[0].to_dict()
        return {"transect_id": None, "lon": None, "lat": None}

    def main_widget(self):
        """Returns the pane representing the current transect view and toggle button."""
        return self.transect_view

    def view_labelled_transects_button(self):
        """Returns the toggle button to view labelled transects."""
        return self.toggle_button

    def view_get_random_transect_button(self):
        """Returns the toggle button to view labelled transects."""
        return self.get_random_transect_button

    def view_get_basemap_button(self):
        """Returns the toggle button to view labelled transects."""
        return self.basemap_button


# class SpatialQueryApp(param.Parameterized):
#     current_transect = param.ClassSelector(
#         class_=gpd.GeoDataFrame, doc="Current transect as a GeoDataFrame"
#     )
#     show_labelled_transects = param.Boolean(
#         default=False, doc="Show/Hide Labelled Transects"
#     )

#     def __init__(self, spatial_engine, labelled_transect_manager, default_geometry):
#         super().__init__()
#         self.spatial_engine = spatial_engine
#         self.labelled_transect_manager = labelled_transect_manager
#         self.view_initialized = False

#         # Initialize map tiles and point drawing tools
#         self.tiles = gvts.EsriImagery()  # Default tiles
#         self.point_draw = gv.Points([]).opts(
#             size=10, color="red", tools=["hover"], responsive=True
#         )

#         # Set the default transect without triggering a view update
#         self.default_geometry = default_geometry
#         self.set_transect(self.default_geometry, query_triggered=False, update=False)

#         # Initialize the UI components (view initialized first)
#         self.transect_view = self.initialize_view()

#         # Mark the view as initialized
#         self.view_initialized = True

#         # Setup the UI after the transect and view are initialized
#         self.setup_ui()

#     def setup_ui(self):
#         """Set up the dynamic visualization and point drawing tools."""
#         self.point_draw_stream = streams.PointDraw(
#             source=self.point_draw, num_objects=1
#         )
#         self.point_draw_stream.add_subscriber(self.on_point_draw)

#         # Add the toggle button to show/hide labelled transects
#         self.toggle_button = pn.widgets.Toggle(
#             name="Show Labelled Transects", value=False, button_type="default"
#         )
#         self.toggle_button.param.watch(self.toggle_labelled_transects, "value")

#         self.get_random_transect_button = pn.widgets.Button(
#             name="Get random transect (slow)", button_type="default"
#         )
#         self.get_random_transect_button.on_click(self._get_random_transect)

#         # Add a radio button group for basemap selection
#         self.basemap_button = pn.widgets.RadioButtonGroup(
#             name="Basemap", options=["Esri Imagery", "OSM"], value="Esri Imagery"
#         )
#         self.basemap_button.param.watch(self.update_basemap, "value")

#     def initialize_view(self):
#         """Initializes the HoloViews pane using the current transect."""
#         return pn.pane.HoloViews(
#             (
#                 self.plot_transect(self.current_transect) * self.tiles * self.point_draw
#             ).opts(active_tools=["wheel_zoom"])
#         )

#     def update_basemap(self, event):
#         """Update the tiles based on the selected basemap."""
#         if event.new == "Esri Imagery":
#             self.tiles = gvts.EsriImagery()
#         elif event.new == "OSM":
#             self.tiles = gvts.OSM()

#         # Update the view to reflect the new tiles
#         self.update_view()

#     def plot_transect(self, transect):
#         """Plot the given transect with polygons and paths."""
#         coords = list(transect.geometry.item().coords)
#         landward_point, seaward_point = coords[0], coords[-1]
#         polygon = gpd.GeoDataFrame(
#             geometry=[
#                 create_offset_rectangle(
#                     transect.to_crs(transect.estimate_utm_crs()).geometry.item(), 200
#                 )
#             ],
#             crs=transect.estimate_utm_crs(),
#         )
#         polygon_plot = gv.Polygons(
#             polygon[["geometry"]].to_crs(4326), label="Area of Interest"
#         ).opts(fill_alpha=0.1, fill_color="green", line_width=2)
#         transect_plot = gv.Path(
#             transect[["geometry"]].to_crs(4326), label="Transect"
#         ).opts(color="red", line_width=1, tools=["hover"])
#         landward_point_plot = gv.Points([landward_point], label="Landward").opts(
#             color="green", line_color="red", size=10
#         )
#         seaward_point_plot = gv.Points([seaward_point], label="Seaward").opts(
#             color="blue", line_color="red", size=10
#         )

#         return (
#             polygon_plot * transect_plot * landward_point_plot * seaward_point_plot
#         ).opts(legend_position="bottom_right")

#     def set_transect(self, transect_data, query_triggered=False, update=True):
#         """Sets the current transect and optionally updates the view."""
#         if isinstance(transect_data, dict):
#             self.current_transect = self.prepare_transect(transect_data)
#         else:
#             self.current_transect = transect_data

#         # Update the view only if explicitly allowed and after initialization
#         if update and self.view_initialized:
#             self.update_view()

#     def update_view(self):
#         """Update the visualization based on the current transect."""
#         new_view = self.plot_transect(self.current_transect)

#         # If show_labelled_transects is True, include labelled transects in the view
#         if self.show_labelled_transects:
#             labelled_transects_plot = (
#                 self.labelled_transect_manager.plot_labelled_transects()
#             )
#             new_view = new_view * labelled_transects_plot

#         self.transect_view.object = (new_view * self.tiles * self.point_draw).opts(
#             legend_position="bottom_right", active_tools=["wheel_zoom"]
#         )

#     def on_point_draw(self, data):
#         """Handle the point draw event and query the nearest geometry based on drawn points."""
#         if data:
#             x, y = data["Longitude"][0], data["Latitude"][0]
#             self.query_and_set_transect(x, y)

#     def query_and_set_transect(self, x, y):
#         """Queries the nearest transect and updates the current transect."""
#         try:
#             geometry = self.spatial_engine.get_nearest_geometry(x, y)
#             self.set_transect(geometry, query_triggered=True)
#         except Exception:
#             logger.exception("Failed to query geometry. Reverting to default transect.")
#             self.set_transect(self.default_geometry, query_triggered=False)

#     def main_widget(self):
#         """Returns the pane representing the current transect view and toggle button."""
#         return pn.Column(self.transect_view, self.basemap_button)

#     def view_labelled_transects_button(self):
#         """Returns the toggle button to view labelled transects."""
#         return self.toggle_button

#     def view_get_random_transect_button(self):
#         """Returns the toggle button to view labelled transects."""
#         return self.get_random_transect_button

#     def view_get_basemap_button(self):
#         """Returns the toggle button to view labelled transects."""
#         return self.basemap_button
