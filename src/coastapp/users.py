import logging

import fsspec
import panel as pn
import param

from coastapp.crud import CRUDManager
from coastapp.specification import User  # noqa

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
        """Add a new user based on input."""
        user_input = self.user_input.value.strip()

        if not user_input:
            self.feedback_message.object = (
                "**Warning:** Please provide a valid name for the user."
            )
            return

        # Check if user already exists
        if any(user == User._format_name(user_input) for user in self.existing_users):
            formatted_name = User._format_name(user_input)
            self.feedback_message.object = (
                f"**Warning:** User '{formatted_name}' already exists."
            )
            self.user_list.value = formatted_name
            return

        # Create and save new user
        user = User(name=user_input)
        if not user.validate():
            self.feedback_message.object = (
                f"**Error:** Invalid user record '{user_input}'."
            )
            return
        self.save_user(user)
        self.feedback_message.object = (
            f"**Success:** User '{user.formatted_name}' added successfully."
        )

        # Update user list and select the new user
        self.existing_users.append(user.formatted_name)
        self.user_list.options = [
            None,
            *self.existing_users,
        ]
        self.user_list.value = user.formatted_name
        self.user_input.value = ""

    def save_user(self, user: User) -> None:
        """Save a user record to storage."""
        fs = fsspec.filesystem("az", **self.storage_options)
        filename = f"{self.base_uri}/user_{user.formatted_name}.json"

        try:
            with fs.open(filename, mode="w") as f:
                f.write(user.to_json())
        except Exception as e:
            logger.error(f"Failed to save user {user.formatted_name}: {e}")
            raise

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
