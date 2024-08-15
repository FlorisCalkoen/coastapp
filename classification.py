import datetime
import logging

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

        # Panel widgets
        self.save_button = pn.widgets.Button(
            name="Save Classification", button_type="primary", disabled=True
        )
        self.is_challenging_button = pn.widgets.Toggle(
            name="Is Challenging", button_type="default", value=False
        )
        self.comment_input = pn.widgets.TextAreaInput(
            name="Optional Comment", placeholder="Enter your comments here..."
        )
        self.link_input = pn.widgets.TextInput(
            name="Optional Link", placeholder="Enter a URL link to a useful resource..."
        )
        self.save_feedback_message = pn.pane.Markdown()

        # Setup callbacks
        self.save_button.on_click(self.save_classification)
        self.classification_schema_manager.attribute_dropdowns[
            "shore_fabric"
        ].param.watch(self.enable_save_button, "value")
        self.classification_schema_manager.attribute_dropdowns[
            "coastal_type"
        ].param.watch(self.enable_save_button, "value")
        self.classification_schema_manager.attribute_dropdowns["defenses"].param.watch(
            self.enable_save_button, "value"
        )

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for classification storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """Generate a filename for the classification record."""
        user = record["user"]
        transect_id = record["transect_id"]
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        return f"{user}_{transect_id}_{timestamp}.json"

    def collect_classification_data(self) -> dict:
        """Collect data from the user manager, classification schema, and spatial query app."""
        # Get the selected user
        user = self.user_manager.selected_user

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

        # Collect spatial data (assuming get_selected_geometry returns a dict with transect_id, lon, and lat)
        spatial_data = self.spatial_query_app.get_selected_geometry()
        transect_id = spatial_data.get("transect_id")
        lon = float(spatial_data.get("lon"))
        lat = float(spatial_data.get("lat"))

        # Create the record
        record = {
            "user": user,
            "transect_id": transect_id,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "shore_fabric": shore_fabric,
            "coastal_type": coastal_type,
            "defenses": defenses,
            "is_challenging": self.is_challenging_button.value,
            "lon": lon,
            "lat": lat,
            "comment": self.comment_input.value,  # Optional comment
            "link": self.link_input.value,  # Optional link
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

    def save_classification(self, event=None):
        """Save the classification data to cloud storage."""
        record = self.collect_classification_data()

        # Validate the record before saving
        if not self.validate_record(record):
            return

        self.create_record(record)
        self.save_feedback_message.object = f"**Success:** Classification saved successfully. File: {self.generate_filename(record)}"
        self.save_button.disabled = True  # Disable save after successful save

        # Reset the dropdowns after successful save
        self.reset_dropdowns()

    def enable_save_button(self, event=None):
        """Enable the save button if all required fields are filled."""
        if all(
            dropdown.value
            for dropdown in self.classification_schema_manager.attribute_dropdowns.values()
        ):
            self.save_button.disabled = False

    def toggle_is_challenging(self, event):
        """Toggle the color of the 'Is Challenging' button based on its state."""
        self.is_challenging_button.button_type = "danger" if event.new else "default"

    def view(self):
        """View for displaying the classification save interface."""
        return pn.Column(
            pn.pane.Markdown("## Save Classification"),
            self.is_challenging_button,
            self.comment_input,  # Add the comment input to the interface
            self.link_input,  # Add the link input to the interface
            self.save_button,
            self.save_feedback_message,
            name="Classification Management",
        )
