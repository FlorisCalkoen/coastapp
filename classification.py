import datetime
import logging

import panel as pn
from crud import CRUDManager

logger = logging.getLogger(__name__)

class ClassificationManager(CRUDManager):
    def __init__(self, storage_options, container_name, prefix, user_manager, classification_schema_manager, spatial_query_app):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix
        self.user_manager = user_manager
        self.classification_schema_manager = classification_schema_manager
        self.spatial_query_app = spatial_query_app

        # Panel widgets
        self.save_button = pn.widgets.Button(name="Save Classification", button_type="primary", disabled=True)
        self.is_challenging_button = pn.widgets.Toggle(name="Is Challenging", button_type="default")
        self.save_feedback_message = pn.pane.Markdown()

        # Setup callbacks
        self.save_button.on_click(self.save_classification)
        self.is_challenging_button.param.watch(self.toggle_is_challenging, 'value')

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for classification storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """Generate a filename for the classification record."""
        user = record['user']
        transect_id = record['transect_id']
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        return f"{user}_{transect_id}_{timestamp}.json"

    def collect_classification_data(self) -> dict:
        """Collect data from the user manager, classification schema, and spatial query app."""
        # Get the selected user
        user = self.user_manager.selected_user

        # Collect selected classes
        shore_fabric = self.classification_schema_manager.attribute_dropdowns['shore_fabric'].value
        coastal_type = self.classification_schema_manager.attribute_dropdowns['coastal_type'].value
        defenses = self.classification_schema_manager.attribute_dropdowns['defenses'].value

        # Collect spatial data (assuming get_selected_geometry returns a dict with transect_id, lon, and lat)
        spatial_data = self.spatial_query_app.get_selected_geometry()
        transect_id = spatial_data.get('transect_id')
        # NOTE: we have to cast to regular floats - otherwise it's not serializable to JSON
        lon = float(spatial_data.get('lon'))
        lat = float(spatial_data.get('lat'))

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
            "comment": "",  # Placeholder for comment
            "link": ""      # Placeholder for link
        }
        return record

    def validate_record(self, record: dict) -> bool:
        """Validate that all required fields are filled."""
        required_fields = ['user', 'transect_id', 'shore_fabric', 'coastal_type', 'defenses', 'lon', 'lat']
        for field in required_fields:
            if not record.get(field):
                self.save_feedback_message.object = f"**Error:** {field.replace('_', ' ').capitalize()} is required."
                return False
        return True

    def save_classification(self, event=None):
        """Save the classification data to cloud storage."""
        record = self.collect_classification_data()

        # Validate the record before saving
        if not self.validate_record(record):
            return

        self.create_record(record)
        self.save_feedback_message.object = (
            f"**Success:** Classification saved successfully. File: {self.generate_filename(record)}"
        )
        self.save_button.disabled = True  # Disable save after successful save

    def toggle_is_challenging(self, event):
        """Toggle the color of the 'Is Challenging' button based on its state."""
        self.is_challenging_button.button_type = 'danger' if event.new else 'default'
        # Re-enable save button if all other fields are filled
        if all(dropdown.value for dropdown in self.classification_schema_manager.attribute_dropdowns.values()):
            self.save_button.disabled = False

    def view(self):
        """View for displaying the classification save interface."""
        return pn.Column(
            pn.pane.Markdown("## Save Classification"),
            self.is_challenging_button,
            self.save_button,
            self.save_feedback_message,
            name="Classification Management",
        )

