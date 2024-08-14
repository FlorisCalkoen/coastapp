import json
import os

import dotenv
import fsspec
import panel as pn

# Load environment variables (e.g., Azure SAS token)
dotenv.load_dotenv(override=True)
sas_token = os.getenv("APPSETTING_GCTS_AZURE_STORAGE_SAS_TOKEN")

class ClassificationSchemaManager:
    def __init__(self):
        # Initialize the schema URL with the https protocol
        self.schema_url = f"https://coclico.blob.core.windows.net/typology/classification-schema.json?{sas_token}"
        print(self.schema_url)
        self.class_mapping = self.load_schema()

        # Panel widgets
        self.attribute_dropdowns = self._initialize_attribute_dropdowns()

    def load_schema(self) -> dict:
        """Load the classification schema from Azure storage using HTTPS."""
        try:
            # Use fsspec with the https protocol
            with fsspec.open(self.schema_url, mode="r") as f:
                schema_data = json.load(f)
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

    def view(self):
        """View for displaying the attribute dropdowns."""
        return pn.Column(*self.attribute_dropdowns.values())

# Initialize the ClassificationSchemaManager and show the attribute dropdowns
classification_schema_manager = ClassificationSchemaManager()

# Serve the Panel app
pn.serve(classification_schema_manager.view)

# import logging

# import panel as pn
# from crud import CRUDManager

# logger = logging.getLogger(__name__)


# class ClassificationSchemaManager(CRUDManager):
#     def __init__(self, storage_options, container_name, prefix=None):
#         super().__init__(
#             container_name=container_name,
#             prefix=prefix,
#             storage_options=storage_options,
#         )

#         # Load the classification schema
#         self.class_mapping = self.load_schema()

#         # Panel widgets
#         self.attribute_dropdowns = self._initialize_attribute_dropdowns()
#         self.class_description_pane = pn.pane.Markdown(
#             "Select an option to see the description."
#         )
#         (
#             self.attribute_selector,
#             self.class_name_input,
#             self.class_description_input,
#             self.add_class_button,
#         ) = self._initialize_class_input_widgets()

#         # Setup callbacks
#         for dropdown in self.attribute_dropdowns.values():
#             dropdown.param.watch(self._on_dropdown_change, "value")
#         self.add_class_button.on_click(self.add_class_to_attribute)

#     @property
#     def base_path(self) -> str:
#         """Defines the base path for classification storage."""
#         if self.prefix:
#             return f"az://{self.container_name}/{self.prefix}/"
#         return f"az://{self.container_name}/"

#     def generate_filename(self, record: dict) -> str:
#         """The filename is simply the classification-schema.json in this case."""
#         return "classification-schema.json"

#     def load_schema(self):
#         """Load the classification schema from the storage backend."""
#         return self.read_record(self.generate_filename({}))

#     def _initialize_attribute_dropdowns(self) -> dict[str, pn.widgets.Select]:
#         """
#         Initialize attribute dropdown widgets.

#         Returns:
#             dict: A dictionary containing attribute dropdown widgets.
#         """
#         dropdown_options = {
#             attribute: [None, *list(classes.keys())]
#             for attribute, classes in self.class_mapping.items()
#         }
#         return {
#             attribute: pn.widgets.Select(
#                 name=attribute,
#                 options=dropdown_options[attribute],
#                 value=None,
#             )
#             for attribute in self.class_mapping
#         }

#     def _on_dropdown_change(self, event):
#         """
#         Callback function to handle change in dropdown value.
#         """
#         markdown_string = ""
#         for attribute, dropdown in self.attribute_dropdowns.items():
#             selected_class = dropdown.value
#             if selected_class:
#                 description = self.class_mapping[attribute].get(selected_class, "")
#                 if description:
#                     markdown_string += (
#                         f"**{attribute} - {selected_class!s}:** {description}\n\n"
#                     )
#         self.class_description_pane.object = (
#             markdown_string or "Select an option to see the description."
#         )

#     def _initialize_class_input_widgets(self):
#         """
#         Initialize widgets for adding new classes.
#         """
#         attribute_selector = pn.widgets.Select(
#             name="Select Attribute", options=list(self.class_mapping.keys())
#         )
#         class_name_input = pn.widgets.TextInput(
#             name="New Class Name", placeholder="Enter new class name"
#         )
#         class_description_input = pn.widgets.TextAreaInput(
#             name="Class Description", placeholder="Enter class description"
#         )
#         add_class_button = pn.widgets.Button(name="Add Class")
#         return (
#             attribute_selector,
#             class_name_input,
#             class_description_input,
#             add_class_button,
#         )

#     def add_class_to_attribute(self, event):
#         """
#         Add a new class to the selected attribute.
#         """
#         selected_attribute = self.attribute_selector.value
#         new_class_name = "enh:" + self.class_name_input.value
#         new_class_description = self.class_description_input.value

#         if (
#             new_class_name
#             and new_class_name not in self.class_mapping[selected_attribute]
#         ):
#             self.class_mapping[selected_attribute][new_class_name] = (
#                 new_class_description
#             )

#             # Save the updated schema
#             self.update_record(self.generate_filename({}), self.class_mapping)

#             # Clear the input fields for next entry
#             self.class_name_input.value = ""
#             self.class_description_input.value = ""

#             # Update the options for the dropdown directly.
#             self.attribute_dropdowns[selected_attribute].options = [
#                 None,
#                 *list(self.class_mapping[selected_attribute].keys()),
#             ]

#     def view(self):
#         return pn.Column(
#             *self.attribute_dropdowns.values(),
#             self.class_description_pane,
#             self.attribute_selector,
#             self.class_name_input,
#             self.class_description_input,
#             self.add_class_button,
#             name="Classification Management",
#         )

# import os

# import dotenv

# dotenv.load_dotenv(override=True)
# sas_token = os.getenv("APPSETTING_GCTS_AZURE_STORAGE_SAS_TOKEN")
# storage_options = {"acount_name": "coclico", "sas_token": sas_token}
# classification_schema_manager = ClassificationSchemaManager(storage_options, "typology")
# pn.Column(classification_schema_manager.attribute_dropdowns.values()).show()
# # pn.serve(classification_schema_manager.view())
