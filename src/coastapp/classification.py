import datetime
import logging
import uuid

import pandas as pd
import panel as pn

from coastapp.crud import CRUDManager
from coastapp.specification import Transect, TypologyTestSample, TypologyTrainSample
from coastapp.shared_state import shared_state

logger = logging.getLogger(__name__)


class ClassificationManager(CRUDManager):
    shared_state = shared_state

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
        self.record = TypologyTrainSample.null()

        # Panel widgets
        self.save_button = pn.widgets.Button(
            name="Save Classification", button_type="success", disabled=True
        )
        self.is_validated_button = pn.widgets.Toggle(
            name="Validated in second assessment", button_type="default", value=False
        )
        self.confidence_slider = pn.widgets.DiscreteSlider(
            options=["low", "medium", "high"], name="Confidence", value="medium"
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

        # Next and Previous buttons
        self.previous_test_button = pn.widgets.Button(
            name="Previous Transect", button_type="default"
        )
        self.next_test_button = pn.widgets.Button(
            name="Next Transect", button_type="default"
        )

        self.uuid_text_input = pn.widgets.TextInput(
            name="Load sample by UUID", placeholder="Enter a UUID here..."
        )

        self.get_random_test_sample_button = pn.widgets.Button(
            name="Get random test sample", button_type="default"
        )

        # Setup callbacks
        self.save_button.on_click(self.save_classification)
        self.is_challenging_button.param.watch(self.toggle_is_challenging, "value")
        self.is_validated_button.param.watch(self.toggle_is_validated, "value")

        # Next and Previous buttons
        self.previous_button.on_click(self.load_previous_transect)
        self.next_button.on_click(self.load_next_transect)
        # test buttons
        self.previous_test_button.on_click(self.load_previous_test_transect)
        self.next_test_button.on_click(self.load_next_test_transect)

        self.uuid_text_input.param.watch(self._load_record_by_uuid, "value")
        self.get_random_test_sample_button.on_click(self._get_random_test_sample)

        self.setup_schema_callbacks()

        self.seen_uuids = []

    def setup_schema_callbacks(self):
        """Setup callbacks for classification schema dropdowns to enable save button."""
        for attr in [
            "shore_type",
            "coastal_type",
            "is_built_environment",
            "has_defense",
        ]:
            if attr in self.classification_schema_manager.attribute_dropdowns:
                self.classification_schema_manager.attribute_dropdowns[
                    attr
                ].param.watch(self.enable_save_button, "value")

    def load_transect_data_into_widgets(self, record):
        """Load transect record data into the classification widgets."""
        # Update the classification widgets based on the fetched record
        # record = self.spatial_query_app.labelled_transect_manager.get_current_record()
        record_as_dict = record.to_dict()
        self.spatial_query_app.current_transect_id = record_as_dict["transect_id"]
        self.classification_schema_manager.attribute_dropdowns[
            "shore_type"
        ].value = record_as_dict.get("shore_type")
        self.classification_schema_manager.attribute_dropdowns[
            "coastal_type"
        ].value = record_as_dict.get("coastal_type")
        self.classification_schema_manager.attribute_dropdowns[
            "is_built_environment"
        ].value = record_as_dict.get("is_built_environment")
        self.classification_schema_manager.attribute_dropdowns[
            "has_defense"
        ].value = record_as_dict.get("has_defense")
        self.confidence_slider.value = record_as_dict.get("confidence", "medium")
        self.is_validated_button.value = record_as_dict.get("is_validated", False)
        self.comment_input.value = record_as_dict.get("comment", "")
        self.link_input.value = record_as_dict.get("link", "")
        self.is_challenging_button.value = record_as_dict.get("is_challenging", False)
        self.spatial_query_app.set_transect(record)

    def reset_record(self):
        """Reset the record to the default schema."""
        self.record = TypologyTrainSample.null()

    def load_previous_transect(self, event=None):
        """Callback to load the previous transect."""
        record = self.spatial_query_app.labelled_transect_manager.get_previous_record(
            dataframe="user_df"
        )
        if record:
            self.record = record
            self.load_transect_data_into_widgets(record)

    def load_next_transect(self, event=None):
        """Callback to load the next transect."""
        record = self.spatial_query_app.labelled_transect_manager.get_next_record(
            "user_df"
        )
        if record:
            self.record = record
            self.load_transect_data_into_widgets(record)

    def load_previous_test_transect(self, event=None):
        """Callback to load the previous transect."""
        record = self.spatial_query_app.labelled_transect_manager.get_previous_record(
            dataframe="test_df"
        )
        if record:
            self.record = record
            self.load_transect_data_into_widgets(record)

    def load_next_test_transect(self, event=None):
        """Callback to load the next transect."""
        record = self.spatial_query_app.labelled_transect_manager.get_next_record(
            "test_df"
        )
        if record:
            self.record = record
            self.load_transect_data_into_widgets(record)

    def iterate_labelled_transects_view(self):
        """Return a Row containing the Previous and Next transect buttons."""
        return pn.Row(self.previous_button, self.next_button)

    def view_iterate_test_transects(self):
        """Return a Row containing the Previous and Next transect buttons."""
        return pn.Row(self.previous_test_button, self.next_test_button)

    def uuid_text_input_view(self):
        """Return an AutocompleteInput widget for UUID entry."""
        return pn.Row(self.uuid_text_input)

    def _load_record_by_uuid(self, event):
        """Callback to load record by UUID if it exists."""
        input_uuid = event.new
        record = self.spatial_query_app.labelled_transect_manager.fetch_record_by_uuid(
            input_uuid
        )

        if record:
            self.load_transect_data_into_widgets(record)
        else:
            # Display message or reset widget to indicate record was not found
            self.uuid_text_input.value = "WARNING: Please enter a valid UUID."

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for classification storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """Generate a filename for the classification record."""
        user = record["user"]
        transect_id = record["transect_id"]
        time = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%S")
        return f"{user}_{transect_id}_{time}.json"

    def collect_classification_data(self) -> TypologyTrainSample:
        """Collect data from the user manager, classification schema, and spatial query app."""
        user = self.user_manager.selected_user.value

        # Collect selected classes
        shore_type = self.classification_schema_manager.attribute_dropdowns[
            "shore_type"
        ].value
        coastal_type = self.classification_schema_manager.attribute_dropdowns[
            "coastal_type"
        ].value
        landform_type = self.classification_schema_manager.attribute_dropdowns[
            "landform_type"
        ].value

        # NOTE: tmp fixup that accounts for empty landform types
        if not landform_type:
            landform_type = "N/A"

        is_built_environment = self.classification_schema_manager.attribute_dropdowns[
            "is_built_environment"
        ].value
        has_defense = self.classification_schema_manager.attribute_dropdowns[
            "has_defense"
        ].value

        # Collect spatial data from the GeoDataFrame in spatial_query_app
        current_transect = self.spatial_query_app.get_selected_geometry().to_dict()
        transect_id = current_transect.get("transect_id")
        lon = current_transect.get("lon")
        lat = current_transect.get("lat")
        geometry = current_transect.get("geometry")
        datetime_created = current_transect.get("datetime_created")

        # Handle missing spatial data
        if pd.isna(transect_id) or pd.isna(lon) or pd.isna(lat) or pd.isna(geometry):
            self.save_feedback_message.object = "**Error:** No valid transect selected."
            return None

        # Determine time values
        current_datetime = datetime.datetime.now(datetime.UTC)

        # NOTE: you cannot use self.record.get("datetime_created", current_datetime)
        # because then you will find the default value, which is an empty string. See
        # default schema. So, if datetime_created is an empty string, set it to current_datetime
        if pd.isna(datetime_created):
            datetime_created = current_datetime
        else:
            datetime_created = pd.Timestamp(datetime_created).to_pydatetime()

        datetime_updated = current_datetime

        universal_unique_id = uuid.uuid4().hex[:12]
        # Collect data locally before updating self.record

        transect = Transect(
            transect_id=transect_id, geometry=geometry, lon=lon, lat=lat
        )
        record = TypologyTrainSample(
            transect=transect,
            user=user,
            uuid=universal_unique_id,
            datetime_created=datetime_created,
            datetime_updated=datetime_updated,
            shore_type=shore_type,
            coastal_type=coastal_type,
            landform_type=landform_type,
            is_built_environment=is_built_environment,
            has_defense=has_defense,
            is_challenging=self.is_challenging_button.value,
            comment=self.comment_input.value,
            link=self.link_input.value,
            confidence=self.confidence_slider.value,
            is_validated=self.is_validated_button.value,
        )

        self.record = record
        return self.record

    def validate_record(self, record: dict) -> bool:
        """Validate that all required fields are filled."""

        required_fields = [
            "user",
            "transect_id",
            "shore_type",
            "coastal_type",
            "is_built_environment",
            "has_defense",
            "lon",
            "lat",
        ]

        # Check if required fields are present and non-empty
        for field in required_fields:
            if not record.get(field):
                self.save_feedback_message.object = (
                    f"**Error:** {field.replace('_', ' ').capitalize()} is required."
                )
                return False

        # Additional validation for numeric fields
        try:
            float(record["lon"])
            float(record["lat"])
        except ValueError:
            self.save_feedback_message.object = (
                "**Error:** Invalid longitude or latitude."
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

        # Reset the 'is_validated' button
        self.is_validated_button.value = False
        self.is_validated_button.button_type = "default"
        self.is_validated = False

        self.comment_input.value = ""
        self.link_input.value = ""

    def save_classification(self, event=None):
        """Save the classification data to cloud storage."""
        record = self.collect_classification_data()

        if not record.validate():
            raise ValueError("Record is not valid")

        # TODO: in future do the validation like above
        if not self.validate_record(record.to_dict()):
            return

        # If labelled transects are in memory, update the in-memory dataframe
        # even though the df is not in memory yet, this check fails, so it will always
        # read the data. Fix that.
        if self.spatial_query_app.labelled_transect_manager._df is not None:
            self.spatial_query_app.labelled_transect_manager.add_record(record)

        # Save the record to cloud storage
        self.create_record(record)
        self.save_feedback_message.object = f"**Success:** Classification saved successfully. File: {self.generate_filename(record.to_dict())}"
        self.save_button.disabled = True  # Disable save after successful save

        # Reset the dropdowns after saving
        self.reset_dropdowns()
        self.reset_record()

    def enable_save_button(self, event=None):
        """Enable the save button if all required fields are filled."""
        required_dropdowns = [
            "shore_type",
            "coastal_type",
            "is_built_environment",
            "has_defense",
        ]
        if all(
            self.classification_schema_manager.attribute_dropdowns[dropdown].value
            for dropdown in required_dropdowns
        ):
            self.save_button.disabled = False
        else:
            self.save_button.disabled = True

    def toggle_is_challenging(self, event):
        """Toggle the 'is_challenging' flag and change button color."""
        self.is_challenging = event.new
        self.is_challenging_button.button_type = (
            "danger" if self.is_challenging else "default"
        )

    def toggle_is_validated(self, event):
        """Toggle the 'is_challenging' flag and change button color."""
        self.is_validated = event.new
        self.is_validated_button.button_type = (
            "success" if self.is_validated else "default"
        )

    def toggle_load_record(self, event):
        """Toggle the 'is_challenging' flag and change button color."""
        if "uuid" in self.spatial_query_app.current_transect.columns:
            record = (
                self.spatial_query_app.labelled_transect_manager.fetch_record_by_uuid(
                    self.spatial_query_app.current_transect.uuid.item()
                )
            )
            if record:
                self.load_transect_data_into_widgets(record)

    def _get_random_test_sample(self, event):
        """Handle the button click to get a random transect from the filtered test_df."""
        test_df = self.spatial_query_app.labelled_transect_manager.test_df

        # Sample one record
        sample = test_df.sample(1)
        self.shared_state.seen_uuids = self.shared_state.seen_uuids + [
            sample["uuid"].item()
        ]

        # Convert to model
        train_sample = TypologyTrainSample.from_frame(sample)
        test_sample = TypologyTestSample(
            train_sample=train_sample,
            pred_shore_type=sample.pred_shore_type.item(),
            pred_coastal_type=sample.pred_coastal_type.item(),
            pred_has_defense=sample.pred_has_defense.item(),
            pred_is_built_environment=sample.pred_is_built_environment.item(),
        )

        try:
            self.load_transect_data_into_widgets(test_sample)
        except Exception:
            logger.exception("Failed to query geometry. Reverting to default transect.")
            self.spatial_query_app.set_transect(self.spatial_query_app.default_geometry)

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

    def view_quality_assurance(self):
        return pn.Column(self.confidence_slider, self.is_validated_button)

    def view_get_random_test_sample(self):
        """Returns the toggle button to view predicted test transects."""
        return self.get_random_test_sample_button
