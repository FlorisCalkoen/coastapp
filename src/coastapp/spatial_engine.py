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
from shapely.geometry import Point
from shapely.wkb import loads

from coastapp.enums import StorageBackend
from coastapp.shared_state import shared_state
from coastapp.specification import (
    BaseModel,
    Transect,
    TypologyTestSample,
    TypologyTrainSample,
)
from coastapp.style_config import COAST_TYPE_COLORS, SHORE_TYPE_MARKERS
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
        if len(self.quadtiles["proj:code"].unique()) > 1:
            raise ValueError("Multiple CRSs found in the STAC collection.")
        self.proj_code = self.quadtiles["proj:code"].unique().item()

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
            "proj:code": stac_item.properties["proj:code"],
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
        point_gdf_wkt = point_gdf.to_crs(self.proj_code).geometry.to_wkt().iloc[0]

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
    current_transect = param.ClassSelector(class_=BaseModel, doc="Current transect")

    # show_labelled_transects = param.Boolean(
    #     default=False, doc="Show/Hide Labelled Transects"
    # )
    # show_test_predictions = param.Boolean(
    #     default=False, doc="Show/Hide Test Prediction Layer"
    # )
    # use_test_storage_backend = param.Boolean(
    #     default=False, doc="Use test storage backend"
    # )
    # only_use_incorrect = param.Boolean(
    #     default=False, doc="Only show incorrect predictions"
    # )
    # only_use_non_validated = param.Boolean(
    #     default=False, doc="Only show incorrect predictions"
    # )
    shared_state = shared_state

    shore_type_markers = SHORE_TYPE_MARKERS
    coast_type_colors = COAST_TYPE_COLORS

    def __init__(self, spatial_engine, labelled_transect_manager):
        super().__init__()
        self.spatial_engine = spatial_engine
        self.labelled_transect_manager = labelled_transect_manager
        self.view_initialized = False
        self.storage_backend = StorageBackend.GCTS

        # Initialize map tiles and point drawing tools
        self.tiles = gvts.EsriImagery()
        self.point_draw = gv.Points([]).opts(
            size=10, color="red", tools=["hover"], responsive=True
        )

        # Set the default transect without triggering a view update

        self.default_geometry = Transect.example()
        self.set_transect(self.default_geometry, update=False)

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

        self.test_predictions_button = pn.widgets.Toggle(
            name="Show Predictions", value=False, button_type="default"
        )
        self.test_predictions_button.param.watch(self.toggle_test_predictions, "value")

        self.only_show_incorrect_predictions_button = pn.widgets.Toggle(
            name="Only show incorrect predictions", value=False, button_type="default"
        )

        self.only_show_incorrect_predictions_button.param.watch(
            self.toggle_only_show_incorrect_predictions, "value"
        )

        self.only_show_non_validated_button = pn.widgets.Toggle(
            name="Only Non-Validated", button_type="default", value=False
        )

        self.only_show_non_validated_button.param.watch(
            self.toggle_only_show_non_validated, "value"
        )

        self.confidence_filter_slider = pn.widgets.DiscreteSlider(
            options=["low", "medium", "high"], name="Confidence Filter", value="medium"
        )

        self.storage_backend_button = pn.widgets.Toggle(
            name="Fetch from predictions", value=False, button_type="default"
        )
        self.storage_backend_button.param.watch(self.toggle_storage_backend, "value")

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
        """
        Plot a transect or test sample by delegating to the appropriate private method.

        Args:
            transect (BaseModel): A Transect, TypologyTrainSample, or TypologyTestSample instance.

        Returns:
            gv.Overlay: A Holoviews overlay object with the visualization layers.
        """
        if isinstance(transect, (Transect, TypologyTrainSample)):
            return self._plot_transect(transect)
        elif isinstance(transect, TypologyTestSample):
            return self._plot_test_transect(transect)
        else:
            raise ValueError(f"Unsupported transect type: {type(transect)}")

    def _plot_transect(self, transect):
        """
        Plot a basic transect or training sample.

        Args:
            transect (BaseModel): A Transect or TypologyTrainSample instance.

        Returns:
            gv.Overlay: A Holoviews overlay object with the visualization layers.
        """
        transect_df = transect.to_frame()

        coords = list(transect_df.geometry.item().coords)
        landward_point, seaward_point = coords[0], coords[-1]

        # Base polygon visualization
        polygon = gpd.GeoDataFrame(
            geometry=[
                create_offset_rectangle(
                    transect_df.to_crs(transect_df.estimate_utm_crs()).geometry.item(),
                    200,
                )
            ],
            crs=transect_df.estimate_utm_crs(),
        )
        polygon_plot = gv.Polygons(
            polygon[["geometry"]].to_crs(4326), label="Area of Interest"
        ).opts(fill_alpha=0.1, fill_color="green", line_width=2)

        if hasattr(transect_df, "shore_type"):
            shore_type = transect_df.shore_type.item()
            coastal_type = transect_df.coastal_type.item()
            has_defense = transect_df.has_defense.item()
            is_built_environment = transect_df.is_built_environment.item()
            label = f"Shore type: {shore_type} \nCoastal type: {coastal_type} \nHas defense: {has_defense} \nBuilt environment: {is_built_environment}"
        else:
            label = "Transect"

        transect_origin = transect_df.lon.item(), transect_df.lat.item()
        transect_origin_plot = gv.Points([transect_origin], label=label).opts(
            color="red"
        )

        # Transect line visualization
        transect_plot = gv.Path(transect_df[["geometry"]].to_crs(4326)).opts(
            color="red", line_width=1, tools=["hover"]
        )

        # Points for landward and seaward locations
        landward_point_plot = gv.Points([landward_point], label="Landward").opts(
            color="green", line_color="red", size=10
        )
        seaward_point_plot = gv.Points([seaward_point], label="Seaward").opts(
            color="blue", line_color="red", size=10
        )

        return (
            polygon_plot
            * transect_origin_plot
            * transect_plot
            * landward_point_plot
            * seaward_point_plot
        ).opts(legend_position="bottom_right")

    def _plot_test_transect(self, test_sample):
        """
        Plot a test transect with prediction-specific layers, including defense and built environment indicators.

        Args:
            test_sample (TypologyTestSample): A TypologyTestSample instance.

        Returns:
            gv.Overlay: A Holoviews overlay object with the visualization layers.
        """
        test_df = test_sample.to_frame()
        if "pred_shore_type" not in test_df.columns:
            raise ValueError("Test sample missing prediction data.")

        # Map classification attributes for shore and coastal types
        test_df["coastal_type_color"] = test_df["pred_coastal_type"].map(
            self.coast_type_colors
        )
        test_df["shore_type_marker"] = test_df["pred_shore_type"].map(
            self.shore_type_markers
        )

        # Set marker size based on pred_has_defense
        test_df["marker_size"] = test_df["pred_has_defense"].apply(
            lambda x: 35 if x == "true" else 25
        )

        # Add circles for defenses (gray) and built environments (red)
        test_df["defense_color"] = test_df["pred_has_defense"].apply(
            lambda x: "#696969" if x == "true" else None
        )
        test_df["built_env_color"] = test_df["pred_is_built_environment"].apply(
            lambda x: "#FF0000" if x == "true" else None
        )

        # Set circle sizes: defense slightly larger than the marker, built environment larger than defense
        test_df["defense_outline_size"] = test_df["marker_size"] + 8
        test_df["built_env_outline_size"] = test_df["defense_outline_size"] + 8

        # Map coordinates for visualization
        test_df["Longitude"] = test_df.lon
        test_df["Latitude"] = test_df.lat

        # Plot the base transect
        transect_plot = self._plot_transect(test_sample)

        # Base prediction markers (shore and coastal type)
        prediction_plot = gv.Points(
            test_df,
            kdims=["Longitude", "Latitude"],
            vdims=[
                "coastal_type_color",
                "pred_shore_type",
                "pred_coastal_type",
                "shore_type_marker",
                "marker_size",
            ],
            label=f"Pred shore type: {test_df['pred_shore_type'].item()} \n Pred coastal type: {test_df['pred_coastal_type'].item()}",
        ).opts(
            color="coastal_type_color",
            marker="shore_type_marker",
            size="marker_size",
            tools=["hover"],
        )

        # Gray outline for defenses
        defense_circles = gv.Points(
            test_df[test_df["pred_has_defense"] == "true"],
            kdims=["Longitude", "Latitude"],
            vdims=["defense_color", "defense_outline_size"],
        ).opts(
            color="defense_color",
            marker="circle",
            size="defense_outline_size",
            fill_color=None,
            line_width=2,
        )

        # Red outline for built environment
        built_env_circles = gv.Points(
            test_df[test_df["pred_is_built_environment"] == "true"],
            kdims=["Longitude", "Latitude"],
            vdims=["built_env_color", "built_env_outline_size"],
        ).opts(
            color="built_env_color",
            marker="circle",
            size="built_env_outline_size",
            fill_color=None,
            line_width=2,
        )

        # Combine plots: built environment circles on top, then defenses, then base points
        final_plot = (
            built_env_circles * defense_circles * prediction_plot * transect_plot
        )

        return final_plot.opts(legend_position="bottom_right")

    # def _plot_test_transect(self, test_sample):
    #     """
    #     Plot a test transect with prediction-specific layers.

    #     Args:
    #         test_sample (TypologyTestSample): A TypologyTestSample instance.

    #     Returns:
    #         gv.Overlay: A Holoviews overlay object with the visualization layers.
    #     """
    #     test_df = test_sample.to_frame()
    #     if "pred_shore_type" not in test_df.columns:
    #         raise ValueError("Test sample missing prediction data.")

    #     test_df["coastal_type_color"] = test_df["pred_coastal_type"].map(
    #         self.coast_type_colors
    #     )
    #     test_df["shore_type_marker"] = test_df["pred_shore_type"].map(
    #         self.shore_type_markers
    #     )

    #     pred_coast_type = test_df["pred_coastal_type"].item()
    #     pred_shore_type = test_df["pred_shore_type"].item()

    #     # Map coordinates for visualization
    #     test_df["Longitude"] = test_df.lon
    #     test_df["Latitude"] = test_df.lat

    #     transect_plot = self._plot_transect(test_sample)

    #     prediction_plot = gv.Points(
    #         test_df,
    #         kdims=["Longitude", "Latitude"],
    #         vdims=[
    #             "coastal_type_color",
    #             "pred_shore_type",
    #             "pred_coastal_type",
    #             "shore_type_marker",
    #         ],
    #         label=f"Pred shore type: {pred_shore_type} \n Pred coastal type: {pred_coast_type}",
    #     ).opts(
    #         color="coastal_type_color",
    #         size=25,
    #         marker="shore_type_marker",
    #     )

    #     return (transect_plot * prediction_plot).opts(legend_position="bottom_right")

    def set_transect(self, data, update=True):
        """Sets the current transect and optionally updates the view."""

        if not isinstance(data, (Transect, TypologyTrainSample, TypologyTestSample)):
            raise ValueError("Data must be an instance of Transect or TypologySample.")

        self.current_transect = data

        # Update the view only if explicitly allowed and after initialization
        if update and self.view_initialized:
            self.update_view()

    def _get_random_transect(self, event):
        """Handle the button click to get a random transect."""
        transect = self.spatial_engine.get_random_transect()
        transect = Transect.from_frame(transect)
        self.set_transect(transect)

    def toggle_labelled_transects(self, event):
        """Handle the toggle button to show or hide labelled transects."""
        self.shared_state.show_labelled_transects = event.new

        if self.shared_state.show_labelled_transects:
            self.toggle_button.button_type = "success"  # Set to green
        else:
            self.toggle_button.button_type = "default"
        self.update_view()

    def toggle_test_predictions(self, event):
        """Handle the toggle button to show or hide labelled transects."""
        self.shared_state.show_test_predictions = event.new

        if self.shared_state.show_test_predictions:
            self.test_predictions_button.button_type = "success"

        else:
            self.test_predictions_button.button_type = "default"
        self.update_view()

    def toggle_only_show_non_validated(self, event):
        """Handle the toggle button to show or hide labelled transects."""
        self.shared_state.only_use_non_validated = event.new

        if self.shared_state.only_use_non_validated:
            self.only_show_non_validated_button.button_type = "success"

        else:
            self.only_show_non_validated_button.button_type = "default"
        self.update_view()

    def toggle_only_show_incorrect_predictions(self, event):
        """Handle the toggle button to show or hide labelled transects."""
        self.shared_state.only_use_incorrect = event.new

        if self.shared_state.only_use_incorrect:
            self.only_show_incorrect_predictions_button.button_type = "success"

        else:
            self.only_show_incorrect_predictions_button.button_type = "default"
        self.update_view()

    def toggle_storage_backend(self, event):
        """Handle the toggle button to show or hide labelled transects."""
        self.shared_state.use_test_storage_backend = event.new

        if self.shared_state.use_test_storage_backend:
            self.storage_backend_button.button_type = "success"  # Set to green
            self.storage_backend = StorageBackend.PREDICTIONS

        else:
            self.storage_backend_button.button_type = "default"
            self.storage_backend = StorageBackend.GCTS

    def update_view(self):
        """
        Update the visualization based on the current transect and settings.
        """
        new_view = self.plot_transect(self.current_transect)

        if self.shared_state.show_labelled_transects:
            labelled_transects_plot = self.plot_labelled_transects()
            new_view = new_view * labelled_transects_plot

        if self.shared_state.show_test_predictions:
            new_view = new_view * self.plot_test_predictions()

        self.transect_view.object = (new_view * self.tiles * self.point_draw).opts(
            legend_position="bottom_right",
            active_tools=["wheel_zoom"],
        )

    def on_point_draw(self, data):
        """Handle the point draw event and query the nearest geometry based on drawn points."""
        if data:
            x, y = data["Longitude"][0], data["Latitude"][0]
            if self.storage_backend == StorageBackend.GCTS:
                self.query_and_set_transect(x, y)
            elif self.storage_backend == StorageBackend.PREDICTIONS:
                self.query_and_set_test_prediction(x, y)

    def query_and_set_transect(self, x, y):
        """Queries the nearest transect and updates the current transect."""
        try:
            geometry = self.spatial_engine.get_nearest_geometry(x, y)
            geometry = Transect.from_frame(geometry)
            self.set_transect(geometry)
        except Exception:
            logger.exception("Failed to query geometry. Reverting to default transect.")
            self.set_transect(self.default_geometry)

    def query_and_set_test_prediction(self, x, y):
        """Queries the nearest transect and updates the current transect."""
        point = (
            gpd.GeoSeries.from_xy([x], [y], crs="EPSG:4326")
            .to_crs("EPSG:3857")
            .to_frame("geometry")
        )
        try:
            df = self.labelled_transect_manager.test_df.copy()
            df = df.to_crs(3857).reset_index(drop=True)
            nearest_transect = gpd.sjoin_nearest(point, df).index_right.item()
            df = df.to_crs(4326)
            geometry = df.iloc[[nearest_transect]]
            geometry = TypologyTestSample.from_frame(geometry)
            self.set_transect(geometry)
        except Exception:
            logger.exception("Failed to query geometry. Reverting to default transect.")
            self.set_transect(self.default_geometry)

    def plot_labelled_transects(self) -> pn.pane.HoloViews:
        """Plot the labelled transects from the loaded GeoDataFrame."""

        # Create a copy of the dataframe for plotting as points
        plot_df = self.labelled_transect_manager.df.copy()

        # Convert to points for plotting
        plot_df = gpd.GeoDataFrame(
            plot_df.drop(columns=["geometry"]),
            geometry=gpd.points_from_xy(plot_df["lon"], plot_df["lat"]),
            crs="EPSG:4326",
        )

        plot = plot_df[["geometry"]].hvplot(
            geo=True,
            color="red",
            responsive=True,
            size=25,
            label="Labelled Transects",
            line_color="green",
        )
        return plot

    def plot_test_prediction(self):
        """Plot the test predictions layer for a single transect with defense and built environment indicators."""

        df = self.current_transect.to_frame()

        if "pred_shore_type" in df.columns:
            # Map existing classification attributes
            df["coastal_type_color"] = df["pred_coastal_type"].map(
                self.coast_type_colors
            )
            df["shore_type_marker"] = df["pred_shore_type"].map(self.shore_type_markers)

            # Adjust marker size based on pred_has_defense
            df["marker_size"] = df["pred_has_defense"].apply(lambda x: 35 if x else 25)

            # Add gray circle outline if pred_is_built_environment is True
            df["is_built_env_color"] = df["pred_is_built_environment"].apply(
                lambda x: "#696969" if x else None
            )
            df["outline_size"] = (
                df["marker_size"] + 8
            )  # Slightly larger than marker size

            # Prepare geometry
            df = df.assign(geometry=gpd.GeoSeries.from_xy(df.lon, df.lat, crs=4326))

            # Base prediction markers (shore and coastal type)
            base_points = gv.Points(
                df,
                kdims=["Longitude", "Latitude"],
                vdims=[
                    "coastal_type_color",
                    "pred_shore_type",
                    "pred_coastal_type",
                    "shore_type_marker",
                    "marker_size",
                ],
                label=f"Shore type: {df['pred_shore_type'].item()} \nCoastal type: {df['pred_coastal_type'].item()}",
            ).opts(
                color="coastal_type_color",
                marker="shore_type_marker",
                size="marker_size",
            )

            # Gray outline for built environment
            outer_circles = gv.Points(
                df[
                    df["pred_is_built_environment"] == True  # noqa: E712
                ],  # Only for built environments
                kdims=["Longitude", "Latitude"],
                vdims=["is_built_env_color", "outline_size"],
            ).opts(
                color="is_built_env_color",
                marker="circle",
                size="outline_size",
                fill_alpha=0,  # Only outline
                line_width=2,
            )

            # Combine plots
            final_plot = outer_circles * base_points

            return final_plot

        else:
            return gv.Points([])

    def plot_test_predictions(self):
        """Plot the test predictions layer for multiple transects with defense and built environment indicators."""

        test_df = self.labelled_transect_manager.test_df.copy()

        # Prepare the GeoDataFrame
        df = (
            test_df.assign(
                geometry=gpd.GeoSeries.from_xy(test_df.lon, test_df.lat, crs=4326)
            )
            .copy()
            .dropna(subset=["user"])
            .reset_index(drop=True)
        )

        # Map classification attributes for shore and coastal types
        df["coastal_type_color"] = df["pred_coastal_type"].map(self.coast_type_colors)
        df["shore_type_marker"] = df["pred_shore_type"].map(self.shore_type_markers)

        # Set marker size based on pred_has_defense
        df["marker_size"] = df["pred_has_defense"].apply(
            lambda x: 35 if x == "true" else 25
        )

        # Add circles for defenses (gray) and built environments (red)
        df["defense_color"] = df["pred_has_defense"].apply(
            lambda x: "#696969" if x == "true" else None
        )
        df["built_env_color"] = df["pred_is_built_environment"].apply(
            lambda x: "#FF0000" if x == "true" else None
        )

        # Set circle sizes: defense slightly larger than the marker, built environment larger than defense
        df["defense_outline_size"] = df["marker_size"] + 8
        df["built_env_outline_size"] = df["defense_outline_size"] + 8

        # Base prediction markers (shore and coastal type)
        base_points = gv.Points(
            df,
            kdims=["Longitude", "Latitude"],
            vdims=[
                "coastal_type_color",
                "pred_shore_type",
                "pred_coastal_type",
                "shore_type_marker",
                "marker_size",
            ],
        ).opts(
            color="coastal_type_color",
            marker="shore_type_marker",
            size="marker_size",
            tools=["hover"],
            legend_position="right",
            width=800,
        )

        # Gray outline for defenses
        defense_circles = gv.Points(
            df[df["pred_has_defense"] == "true"],
            kdims=["Longitude", "Latitude"],
            vdims=["defense_color", "defense_outline_size"],
        ).opts(
            color="defense_color",
            marker="circle",
            size="defense_outline_size",
            fill_color=None,
            line_width=2,
        )

        # Red outline for built environment
        built_env_circles = gv.Points(
            df[df["pred_is_built_environment"] == "true"],
            kdims=["Longitude", "Latitude"],
            vdims=["built_env_color", "built_env_outline_size"],
        ).opts(
            color="built_env_color",
            marker="circle",
            size="built_env_outline_size",
            fill_color=None,
            line_width=2,
        )

        # Combine plots: built environment circles on top, then defenses, then base points
        final_plot = built_env_circles * defense_circles * base_points

        return final_plot

    def get_selected_geometry(self):
        """Returns the currently selected transect's geometry and metadata."""
        return self.current_transect

    def main_widget(self):
        """Returns the pane representing the current transect view and toggle button."""
        return self.transect_view

    def view_labelled_transects_button(self):
        """Returns the toggle button to view labelled transects."""
        return self.toggle_button

    def view_test_predictions_button(self):
        """Returns the toggle button to view predicted test transects."""
        return self.test_predictions_button

    def view_storage_backend_button(self):
        """Returns the toggle button to view predicted test transects."""
        return self.storage_backend_button

    def view_benchmark_backend_button(self):
        """Returns the toggle button to view predicted test transects."""
        return self.benchmark_backend_button

    def view_get_random_transect_button(self):
        """Returns the toggle button to view labelled transects."""
        return self.get_random_transect_button

    def view_get_basemap_button(self):
        """Returns the toggle button to view labelled transects."""
        return self.basemap_button

    def view_test_layer_select(self):
        """Returns the toggle button to view predicted test transects."""
        return self.labelled_transect_manager.test_layer_select

    def view_benchmark_layer_select(self):
        """Returns the toggle button to view predicted test transects."""
        return self.labelled_transect_manager.benchmark_layer_select

    def view_filter_test_predictions(self):
        """Returns the toggle button to view predicted test transects."""
        return pn.Column(
            self.only_show_incorrect_predictions_button,
            self.only_show_non_validated_button,
            self.confidence_filter_slider,
        )

    def view_only_show_incorrect_predictions(self):
        """Returns the toggle button to view predicted test transects."""
        return self.only_show_incorrect_predictions_button
