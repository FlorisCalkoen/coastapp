import logging

import panel as pn
from coastapp.crud import CRUDManager

logger = logging.getLogger(__name__)


class ClassificationSchemaManager(CRUDManager):
    def __init__(self, storage_options, container_name, prefix):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix

        # Load the classification schema using the read_record method
        self.class_mapping = self.load_schema()

        # Panel widgets
        self.attribute_dropdowns = self._initialize_attribute_dropdowns()
        self.classification_display_pane = pn.pane.Markdown(
            "**Current Classification:**\n\nSelect an option to see the description."
        )

        (
            self.attribute_selector,
            self.class_name_input,
            self.class_description_input,
            self.add_class_button,
        ) = self._initialize_class_input_widgets()

        # Setup callbacks
        for dropdown in self.attribute_dropdowns.values():
            dropdown.param.watch(self._on_dropdown_change, "value")
        self.add_class_button.on_click(self.add_class_to_attribute)

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for the classification schema storage."""
        return self.prefix

    def generate_filename(self, record: dict = None) -> str:
        """Generate a filename for the classification schema."""
        return "classification-schema.json"

    def load_schema(self) -> dict:
        """Load the classification schema from Azure storage using the read_record method."""
        try:
            schema_data = self.read_record(self.generate_filename())
            print("Schema successfully loaded from cloud.")
            return schema_data
        except Exception as e:
            print(f"Error loading schema: {e}")
            return {}

    def _initialize_attribute_dropdowns(self) -> dict[str, pn.widgets.Select]:
        """
        Initialize attribute dropdown widgets.

        Returns:
            dict: A dictionary containing attribute dropdown widgets.
        """
        dropdown_options = {
            attribute: [None, *list(classes.keys())]
            for attribute, classes in self.class_mapping.items()
        }
        return {
            attribute: pn.widgets.Select(
                name=attribute,
                options=dropdown_options[attribute],
                value=None,
            )
            for attribute in self.class_mapping
        }

    def _on_dropdown_change(self, event):
        """
        Callback function to handle change in dropdown value and update the classification display pane.
        """
        classification_string = "**Current Classification:**\n\n"
        description_string = ""

        for attribute, dropdown in self.attribute_dropdowns.items():
            selected_class = dropdown.value
            if selected_class:
                description = self.class_mapping[attribute].get(selected_class, "")
                if description:
                    classification_string += (
                        f"**{attribute} - {selected_class}:** {description}\n\n"
                    )

        self.classification_display_pane.object = (
            classification_string
            or "**Current Classification:**\n\nSelect an option to see the description."
        )

    def _initialize_class_input_widgets(self):
        """
        Initialize widgets for adding new classes.
        """
        attribute_selector = pn.widgets.Select(
            name="Select Attribute", options=list(self.class_mapping.keys())
        )
        class_name_input = pn.widgets.TextInput(
            name="New Class Name", placeholder="Enter new class name"
        )
        class_description_input = pn.widgets.TextAreaInput(
            name="Class Description", placeholder="Enter class description"
        )
        add_class_button = pn.widgets.Button(name="Add Class")
        return (
            attribute_selector,
            class_name_input,
            class_description_input,
            add_class_button,
        )

    def add_class_to_attribute(self, event):
        """
        Add a new class to the selected attribute.
        """
        selected_attribute = self.attribute_selector.value
        new_class_name = "enh:" + self.class_name_input.value.strip()
        new_class_description = self.class_description_input.value.strip()

        if (
            new_class_name
            and new_class_name not in self.class_mapping[selected_attribute]
        ):
            self.class_mapping[selected_attribute][new_class_name] = (
                new_class_description
            )

            # Save the updated schema
            self.update_record(self.generate_filename(), self.class_mapping)

            # Clear the input fields for next entry
            self.class_name_input.value = ""
            self.class_description_input.value = ""

            # Update the options for the dropdown directly.
            self.attribute_dropdowns[selected_attribute].options = [
                None,
                *list(self.class_mapping[selected_attribute].keys()),
            ]

            # Refresh the dropdowns
            self.attribute_dropdowns[selected_attribute].param.trigger("options")

    def view_main_widget(self):
        """View for displaying the full UI."""
        return pn.Column(
            pn.pane.Markdown(
                "## Classify this Transect\n"
                "Choose the appropriate class from the options provided, considering "
                "the area covered by the polygon. If no suitable class exists, consider "
                "proposing a new one using the widget below."
            ),
            *self.attribute_dropdowns.values(),
            self.classification_display_pane,
            name="Classification Management",
        )

    def view_add_new_class_widget(self):
        return pn.Column(
            pn.pane.Markdown(
                "## [Optional] Propose a new class: \n Please ensure the class is essential, as"
                "we aim to keep the number of classes minimal. Your input is valued."
            ),
            self.attribute_selector,
            self.class_name_input,
            self.class_description_input,
            self.add_class_button,
            name="Add New Class",
        )
