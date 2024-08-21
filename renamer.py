import datetime
import json
import logging
import os

import dotenv
import fsspec
from crud import CRUDManager

logger = logging.getLogger(__name__)

# Load environment variables (e.g., SAS tokens)
dotenv.load_dotenv(override=True)
sas_token = os.getenv("APPSETTING_GCTS_AZURE_STORAGE_SAS_TOKEN")
storage_options = {"account_name": "coclico", "sas_token": sas_token}


class Renamer(CRUDManager):
    def __init__(self, container_name: str, storage_options: dict[str, str]):
        super().__init__(container_name, storage_options)

    @property
    def get_prefix(self) -> str:
        """Returns the base prefix for the current implementation."""
        return "labels"

    def generate_filename(self, record: dict) -> str:
        """
        Generates the filename based on user, transect_id, and the record's timestamp.
        """
        user = record["user"]
        transect_id = record["transect_id"]
        timestamp = record.get("time", datetime.datetime.now(datetime.UTC).isoformat())  # Handle "time" key

        # Ensure the timestamp is a datetime object, if it's a string convert it
        if isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp)

        # Format the timestamp to match the format you want (ISO format, without special characters)
        formatted_timestamp = timestamp.strftime("%Y%m%dT%H%M%S")

        return f"{user}_{transect_id}_{formatted_timestamp}.json"

    def rename_keys_and_values(
        self,
        record: dict,
        key_mapping: dict[str, str],
        value_mapping: dict[str, dict[str, str]],
    ) -> dict:
        """
        Renames keys and values in the record based on provided mappings.
        Also adds new attributes like `is_built_environment` and `landform_type`.
        """
        # Rename keys
        for old_key, new_key in key_mapping.items():
            if old_key in record:
                record[new_key] = record.pop(old_key)

        # Rename values
        for key, replacements in value_mapping.items():
            if key in record and record[key] in replacements:
                record[key] = replacements[record[key]]

        # Set the `is_built_environment` attribute based on coastal_type
        if "built-up area" in record.get("coastal_type", "").lower():
            record["is_built_environment"] = True
        else:
            record["is_built_environment"] = False

        # Add `landform_type` attribute as an empty string (for future use)
        record["landform_type"] = ""

        return record

    def _build_storage_path(self, prefix: str, record_name: str) -> str:
        """
        Build the full storage path for a record based on the prefix.
        """
        return f"az://{self.container_name}/{prefix}/{record_name}"

    def process_records(
        self,
        key_mapping: dict[str, str],
        value_mapping: dict[str, dict[str, str]],
        new_prefix: str | None = None,
    ):
        """
        Reads, renames, and saves records in cloud storage.
        """
        prefix_to_use = new_prefix if new_prefix else self.get_prefix
        fs = fsspec.filesystem("az", **self.storage_options)
        labelled_files = fs.glob(f"{self.base_uri}/*.json")

        for file in labelled_files:
            record_name = file.split("/")[-1]
            try:
                # Read the original record
                record = self.read_record(record_name)

                # Rename keys and values, and add new attributes like `is_built_environment` and `landform_type`
                updated_record = self.rename_keys_and_values(
                    record, key_mapping, value_mapping
                )

                # Save the updated record with the correct prefix
                self.save_record_with_prefix(updated_record, record_name, prefix_to_use)

            except Exception as e:
                logger.error(
                    f"Failed to process record {record_name}: {e}", exc_info=True
                )

    def save_record_with_prefix(self, record: dict, record_name: str, prefix: str):
        """
        Saves a record under a specified prefix.
        """
        # Use the CRUDManager method for saving, but with a specific prefix
        full_path = self._build_storage_path(prefix, record_name)

        try:
            # Save the record using the az:// protocol
            record_json = json.dumps(record, indent=4)
            with fsspec.open(full_path, mode="w", **self.storage_options) as f:
                f.write(record_json)
            logger.info(f"Saved record: {full_path}")
        except Exception as e:
            logger.error(f"Failed to save record to {full_path}: {e}", exc_info=True)


if __name__ == "__main__":
    manager = Renamer("typology", storage_options)

    # Define key and value mappers based on original and revised classifications
    key_mapping = {
        "timestamp": "time",  # Renamed to "time"
        "shore_fabric": "shore_type",  # Renamed to "shore_type"
        "defenses": "has_defense",  # Renamed to "has_defenses"
    }

    value_mapping = {
        "coastal_type": {
            "Estuary inlet": "coastal_inlet",  # Adjusted name in revised schema
            "Tidal flat's, including marshes, mangroves and sabkha's.": "coastal_wetlands",  # Consolidated class
            "Coastal plain without built-up areas": "coastal_sediment_plain",  # Corrected name, no built-up reference
            "Coastal plain with built-up area": "coastal_sediment_plain",  # Same class, logic for built-up is separate
        },
    }

    # Process records and save to a different prefix for testing (e.g., "typology/labels2")
    manager.process_records(key_mapping, value_mapping, new_prefix="labels2")
