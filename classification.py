import datetime
import logging

import pandas as pd
import panel as pn
from crud import CRUDManager

logger = logging.getLogger(__name__)


class ClassificationManager(CRUDManager):
    def __init__(
        self,
        storage_options,
        container_name,
        prefix,
        user_manager,
        classification_schema_manager,
        spatial_query_app,
    ):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix
        self.user_manager = user_manager
        self.classification_schema_manager = classification_schema_manager
        self.spatial_query_app = spatial_query_app
        self.is_challenging = False

        # Panel widgets
        self.save_button = pn.widgets.Button(
            name="Save Classification", button_type="success", disabled=True
        )
        self.is_challenging_button = pn.widgets.Toggle(
            name="Flag as Challenging", button_type="default", value=False
        )

        self.comment_input = pn.widgets.TextAreaInput(
            name="Comment", placeholder="[Optional] Enter your comments here..."
        )
        self.link_input = pn.widgets.TextInput(
            name="Link",
            placeholder="[Optional] Add a URL link to a useful resource, like a street-view image.",
        )
        self.save_feedback_message = pn.pane.Markdown()

        # Next and Previous buttons
        self.previous_button = pn.widgets.Button(
            name="Previous Transect", button_type="default"
        )
        self.next_button = pn.widgets.Button(
            name="Next Transect", button_type="default"
        )

        # Setup callbacks
        self.save_button.on_click(self.save_classification)
        self.is_challenging_button.param.watch(self.toggle_is_challenging, "value")
        self.previous_button.on_click(self.load_previous_transect)
        self.next_button.on_click(self.load_next_transect)

        self.setup_schema_callbacks()

    def setup_schema_callbacks(self):
        """Setup callbacks for classification schema dropdowns to enable save button."""
        for attr in ["shore_fabric", "coastal_type", "defenses"]:
            if attr in self.classification_schema_manager.attribute_dropdowns:
                self.classification_schema_manager.attribute_dropdowns[
                    attr
                ].param.watch(self.enable_save_button, "value")

    def load_transect_data_into_widgets(self, record):
        """Load transect record data into the classification widgets."""
        # Update the classification widgets based on the fetched record
        self.spatial_query_app.current_transect_id = record["transect_id"]
        self.classification_schema_manager.attribute_dropdowns[
            "shore_fabric"
        ].value = record.get("shore_fabric")
        self.classification_schema_manager.attribute_dropdowns[
            "coastal_type"
        ].value = record.get("coastal_type")
        self.classification_schema_manager.attribute_dropdowns[
            "defenses"
        ].value = record.get("defenses")
        self.comment_input.value = record.get("comment", "")
        self.link_input.value = record.get("link", "")
        self.is_challenging_button.value = record.get("is_challenging", False)
        self.spatial_query_app.set_transect(record)

    def load_previous_transect(self, event=None):
        """Callback to load the previous transect."""
        record = self.spatial_query_app.labelled_transect_manager.get_previous_record()
        if record:
            self.load_transect_data_into_widgets(record)

    def load_next_transect(self, event=None):
        """Callback to load the next transect."""
        record = self.spatial_query_app.labelled_transect_manager.get_next_record()
        if record:
            self.load_transect_data_into_widgets(record)

    def iterate_labelled_transects_view(self):
        """Return a Row containing the Previous and Next transect buttons."""
        return pn.Row(self.previous_button, self.next_button)

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for classification storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """Generate a filename for the classification record."""
        user = record["user"]
        transect_id = record["transect_id"]
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%S")
        return f"{user}_{transect_id}_{timestamp}.json"

    def collect_classification_data(self) -> dict:
        """Collect data from the user manager, classification schema, and spatial query app."""
        user = self.user_manager.selected_user.value

        # Collect selected classes
        shore_fabric = self.classification_schema_manager.attribute_dropdowns[
            "shore_fabric"
        ].value
        coastal_type = self.classification_schema_manager.attribute_dropdowns[
            "coastal_type"
        ].value
        defenses = self.classification_schema_manager.attribute_dropdowns[
            "defenses"
        ].value

        # Collect spatial data from the GeoDataFrame in spatial_query_app
        spatial_data = self.spatial_query_app.get_selected_geometry()
        transect_id = spatial_data.get("transect_id")
        lon = spatial_data.get("lon")
        lat = spatial_data.get("lat")
        geometry = spatial_data.get("geometry")

        # Handle missing spatial data
        if pd.isna(transect_id) or pd.isna(lon) or pd.isna(lat) or pd.isna(geometry):
            self.save_feedback_message.object = "**Error:** No valid transect selected."
            return None

        # Create the record
        record = {
            "user": user,
            "transect_id": transect_id,
            "lon": float(lon),  # Convert to float for JSON serialization
            "lat": float(lat),  # Convert to float for JSON serialization
            "geometry": geometry.wkt,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "shore_fabric": shore_fabric,
            "coastal_type": coastal_type,
            "defenses": defenses,
            "is_challenging": self.is_challenging,
            "comment": self.comment_input.value,
            "link": self.link_input.value,
        }
        return record

    def validate_record(self, record: dict) -> bool:
        """Validate that all required fields are filled."""
        required_fields = [
            "user",
            "transect_id",
            "shore_fabric",
            "coastal_type",
            "defenses",
            "lon",
            "lat",
        ]
        for field in required_fields:
            if not record.get(field):
                self.save_feedback_message.object = (
                    f"**Error:** {field.replace('_', ' ').capitalize()} is required."
                )
                return False
        return True

    def reset_dropdowns(self):
        """Reset all dropdowns to their default (empty) values."""
        for dropdown in self.classification_schema_manager.attribute_dropdowns.values():
            dropdown.value = None

        # Reset the 'is_challenging' button
        self.is_challenging_button.value = False
        self.is_challenging_button.button_type = "default"
        self.is_challenging = False

    def save_classification(self, event=None):
        """Save the classification data to cloud storage."""
        record = self.collect_classification_data()

        if not record:
            return

        if not self.validate_record(record):
            return

        # If labelled transects are in memory, update the in-memory dataframe
        if self.spatial_query_app.labelled_transect_manager.df is not None:
            self.spatial_query_app.labelled_transect_manager.add_record(record)

        # Save the record to cloud storage
        self.create_record(record)
        self.save_feedback_message.object = f"**Success:** Classification saved successfully. File: {self.generate_filename(record)}"
        self.save_button.disabled = True  # Disable save after successful save

        # Reset the dropdowns after saving
        self.reset_dropdowns()

    def enable_save_button(self, event=None):
        """Enable the save button if all required fields are filled."""
        required_dropdowns = ["shore_fabric", "coastal_type", "defenses"]
        if all(
            self.classification_schema_manager.attribute_dropdowns[dropdown].value
            for dropdown in required_dropdowns
        ):
            self.save_button.disabled = False

    def toggle_is_challenging(self, event):
        """Toggle the 'is_challenging' flag and change button color."""
        self.is_challenging = event.new
        self.is_challenging_button.button_type = (
            "danger" if self.is_challenging else "default"
        )

    def view(self):
        """View for displaying the classification save interface."""
        return pn.Column(
            self.is_challenging_button,
            self.comment_input,
            self.link_input,
            self.save_button,
            self.save_feedback_message,
            name="Classification Management",
        )
