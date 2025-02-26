import logging
import os

import duckdb
import holoviews as hv
import panel as pn
from dotenv import load_dotenv

from coastapp.classification import ClassificationManager
from coastapp.feature import FeatureManager
from coastapp.labels import LabelledTransectManager
from coastapp.schema import ClassificationSchemaManager
from coastapp.spatial_engine import SpatialQueryApp, SpatialQueryEngine
from coastapp.users import UserManager

# Load environment variables
load_dotenv(override=True)
sas_token = os.getenv("APPSETTING_GCTS_AZURE_STORAGE_SAS_TOKEN")
storage_options = {"account_name": "coclico", "sas_token": sas_token}

# Logger setup
logger = logging.getLogger(__name__)
logger.info(f"DuckDB version: {duckdb.__version__}")

pn.extension()
hv.extension("bokeh")

stac_url = "https://coclico.blob.core.windows.net/stac/test/catalog.json"

spatial_engine = SpatialQueryEngine(
    stac_url=stac_url,
    collection_id="gcts",
    storage_backend="azure",
    storage_options=storage_options,
)

# Initialize managers with the new app implementation
user_manager = UserManager(
    storage_options=storage_options, container_name="typology", prefix="users"
)

# Initialize the LabelledTransectManager
labelled_transect_manager = LabelledTransectManager(
    storage_options=storage_options,
    container_name="typology",
    prefix="labels",
    user_manager=user_manager,
)

# Initialize the core application logic
spatial_query_app = SpatialQueryApp(
    spatial_engine=spatial_engine,
    labelled_transect_manager=labelled_transect_manager,
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

intro_pane = pn.pane.Markdown("""
    The Coastal Typology Annotation Tool is designed to collect a crowd-sourced dataset for training machine learning models that help classify coasts and improve our understanding of coastal erosion on large spatial scales. Although the focus is on European coastlines, users are welcome to contribute labels for other continents as well.

    Use the point-draw tool (three dots with an arrow) from the drop-down menu to the right of the map to select a point and fetch its nearest transect. When classifying, base your annotations on the area of interest which is shown by the polygon.

    The classification focuses on four key attributes:

    - **Shore Type**: Describes the material composing the shore (e.g., sandy sediments, rocky formations, or muddy sediments).
    - **Coastal Type**: Refers to the geomorphological features of the coast, which may be natural (e.g., cliffs, dunes) or human-influenced (e.g., engineered structures).
    - **Built Environment**: Indicates whether the coastal area is dominated by human-made structures or remains largely natural.
    - **Defenses**: Determines whether coastal defense structures (e.g., sea walls, breakwaters) are present to protect against erosion and flooding.
""")

# Combine additional features in one column
additional_features_view = pn.Column(
    pn.pane.Markdown("## Additional Features"),
    spatial_query_app.view_labelled_transects_button(),
    classification_manager.iterate_labelled_transects_view(),
    classification_manager.uuid_text_input_view(),
    spatial_query_app.view_get_random_transect_button(),
    spatial_query_app.view_get_basemap_button(),
    feature_manager.view(),
    name="Additional Features",
)

benchmark_samples_view = pn.Column(
    pn.pane.Markdown("## [Advanced:] Human benchmark "),
    spatial_query_app.view_benchmark_layer_select(),
    classification_manager.view_iterate_benchmark_transects(),
    name="[Advanced]: Human benchmark",
)


test_predictions_view = pn.Column(
    pn.pane.Markdown("## [Advanced:] Explore the test predictions"),
    spatial_query_app.view_test_layer_select(),
    spatial_query_app.view_test_predictions_button(),
    spatial_query_app.view_storage_backend_button(),
    spatial_query_app.view_filter_test_predictions(),
    classification_manager.view_iterate_test_transects(),
    classification_manager.view_get_random_test_sample(),
    name="[Advanced]: Explore the test predictions",
)

# Define the Panel template
app = pn.template.FastListTemplate(
    title="Coastal Typology Annotation Tool",
    sidebar=[
        user_manager.view(),
        classification_schema_manager.view_main_widget(),
        classification_manager.view_quality_assurance(),
        classification_schema_manager.view_classification_display_pane(),
        classification_manager.view(),
        additional_features_view,
        classification_schema_manager.view_add_new_class_widget(),
        benchmark_samples_view,
        test_predictions_view,
    ],
    main=[
        intro_pane,
        spatial_query_app.main_widget(),
    ],
    accent_base_color="#007BFF",
    header_background="#007BFF",
)

app.servable().show()
print("Done")
