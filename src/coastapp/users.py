import datetime
import logging
import re
import unicodedata
import uuid

import fsspec
import panel as pn
import param

from coastapp.crud import CRUDManager

logger = logging.getLogger(__name__)


class UserName(param.Parameterized):
    value = param.String(default="", allow_None=True, doc="Name of the current user")


class UserManager(CRUDManager):
    def __init__(self, storage_options, container_name, prefix):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix

        # Reactive user name parameter
        self.selected_user = UserName()

        # Load existing users
        self.existing_users = self.load_existing_users()

        # Panel widgets
        self.user_list = pn.widgets.Select(
            name="User", options=[None, *self.existing_users]
        )
        self.user_input = pn.widgets.TextInput(
            name="Add New User", placeholder="Enter new user name (first and last)"
        )
        self.add_user_button = pn.widgets.Button(name="Add User", button_type="primary")
        self.feedback_message = pn.pane.Markdown()

        # Setup callbacks
        self.user_list.param.watch(self.select_user, "value")
        self.add_user_button.on_click(self.add_new_user)

        # Watch for changes in the reactive selected user
        self.selected_user.param.watch(self._trigger_user_change, "value")

    def _trigger_user_change(self, event):
        """Handle actions that should occur when the selected user changes."""
        logger.info(f"User changed to {self.selected_user.value}")
        # Trigger other updates or actions here, such as updating UI components

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for user storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """Generate a filename for the user based on their formatted name."""
        formatted_name = record["formatted_name"]
        return f"user_{formatted_name}.json"

    def format_name(self, name: str) -> str:
        """Formats the user name by converting to lowercase, removing accents,
        removing special characters, and replacing spaces with hyphens."""
        # Normalize the string to NFD (Normalization Form Decomposition) to break characters into base and accent parts
        name = unicodedata.normalize("NFD", name)
        # Remove accents by filtering out the combining diacritical marks
        name = "".join(char for char in name if unicodedata.category(char) != "Mn")
        # Convert to lowercase
        name = name.lower()
        # Replace spaces with hyphens
        name = re.sub(r"\s+", "-", name)
        # Remove any remaining characters that are not alphanumeric or hyphens
        name = re.sub(r"[^a-z0-9\-]", "", name)
        return name

    def load_existing_users(self):
        """Load all existing users from the storage backend using the az:// protocol."""
        fs = fsspec.filesystem("az", **self.storage_options)
        user_files = fs.glob(f"{self.base_uri}/user_*.json")
        users = [
            user.split("/")[-1].replace("user_", "").replace(".json", "")
            for user in user_files
        ]
        return users

    def add_new_user(self, event=None):
        user_input = self.user_input.value.strip()

        # Check if the user input is empty
        if not user_input:
            self.feedback_message.object = (
                "**Warning:** Please provide a valid name for the user."
            )
            return

        formatted_name = self.format_name(user_input)

        # Check if user already exists
        if formatted_name in self.existing_users:
            self.feedback_message.object = (
                f"**Warning:** User '{formatted_name}' already exists."
            )
            self.user_list.value = formatted_name
            self.user_list.param.trigger("value")
            self.selected_user.value = formatted_name  # Update the reactive user param
            self.user_input.value = ""  # Clear input after adding

        else:
            user_id = str(uuid.uuid4())  # Use UUID for user ID
            record = {
                "name": user_input,
                "formatted_name": formatted_name,
                "user_id": user_id,
                "datetime_created": datetime.datetime.now(datetime.UTC).isoformat(),
            }
            self.create_record(record)
            self.feedback_message.object = (
                f"**Success:** User '{formatted_name}' added successfully."
            )
            # Update user list and select the new user
            self.existing_users.append(formatted_name)
            self.user_list.options = [None, *self.existing_users]
            self.user_list.value = formatted_name
            self.user_list.param.trigger("options")
            self.user_list.param.trigger("value")
            self.selected_user.value = formatted_name  # Update the reactive user param
            self.user_input.value = ""  # Clear input after adding

    def select_user(self, event):
        """Handles the selection of an existing user."""
        self.selected_user.value = event.new  # Update the reactive user param
        if self.selected_user.value:
            self.feedback_message.object = (
                f"**Info:** User '{self.selected_user.value}' selected."
            )

    def view(self):
        return pn.Column(
            self.user_list,
            self.user_input,
            self.add_user_button,
            self.feedback_message,
            name="User Management",
        )
