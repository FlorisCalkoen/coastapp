import datetime
import json
import logging
import os
from copy import deepcopy

import dotenv
import fsspec

from coastapp.crud import CRUDManager

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
        timestamp = record.get(
            "time", datetime.datetime.now(datetime.UTC).isoformat()
        )  # Handle "time" key

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
        value_mapping: dict[str, dict[str, str | bool]],
    ) -> dict:
        """
        Renames keys and values in the record based on provided mappings.
        Works on a copy of the record to avoid in-place modifications.
        """

        # Create a deep copy of the record to avoid modifying the original
        updated_record = deepcopy(record)

        # Rename keys
        for old_key, new_key in key_mapping.items():
            if old_key in updated_record:
                updated_record[new_key] = updated_record.pop(old_key)

        # Rename values
        for key, replacements in value_mapping.items():
            if key in updated_record and updated_record[key] in replacements:
                updated_record[key] = replacements[updated_record[key]]

        return updated_record

    def _build_storage_path(self, prefix: str, record_name: str) -> str:
        """
        Build the full storage path for a record based on the prefix.
        """
        return f"az://{self.container_name}/{prefix}/{record_name}"

    def set_built_environment_flag(self, record: dict) -> bool:
        """
        Determines whether the coastal area is built-up based on the coastal_type.
        """
        built_environment_classes = [
            "enh:Coastal bedrock plain with built-up area",
            "Coastal plain with built-up area",
        ]
        original_coastal_type = record.get("coastal_type", "")
        return original_coastal_type in built_environment_classes

    def process_records(
        self,
        key_mapping: dict[str, str],
        value_mapping: dict[str, dict[str, str | bool]],
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

                # Generate the updated record
                updated_record = self.rename_keys_and_values(
                    record, key_mapping, value_mapping
                )

                # # Set the `is_built_environment` attribute based on original coastal_type
                # updated_record["is_built_environment"] = (
                #     self.set_built_environment_flag(record)
                # )

                # # Set additional fields
                # updated_record["landform_type"] = ""  # Add landform logic as needed

                # Save the updated record with the correct prefix

                updated_record["datetime_updated"] = updated_record["datetime_created"]

                sort_order = [
                    "user",
                    "transect_id",
                    "lon",
                    "lat",
                    "geometry",
                    "datetime_created",
                    "datetime_updated",
                    "shore_type",
                    "coastal_type",
                    "landform_type",
                    "is_built_environment",
                    "has_defense",
                    "is_challenging",
                    "comment",
                    "link",
                ]

                updated_record = {
                    k: updated_record[k] for k in sort_order if k in updated_record
                }

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

    key_mapping = {
        # "shore_fabric": "shore_type",  # Renamed to "shore_type"
        # "defenses": "has_defense",  # Renamed to "has_defense"
        "time": "datetime_created",  # Renamed to "time"
    }

    value_mapping = {
        #     "shore_type": {
        #         "Sandy, gravel or small boulder sediments": "sandy_gravel_or_small_boulder_sediments",
        #         "Muddy sediments": "muddy_sediments",
        #         "Rocky shore platform or large boulders": "rocky_shore_platform_or_large_boulders",
        #         "Ice/tundra": "ice_or_tundra",
        #         "No sediment or shore platform": "no_sediment_or_shore_platform",
        #     },
        #     "coastal_type": {
        #         "Cliffed or steep coasts": "cliffed_or_steep_coasts",
        #         "Dune coast": "dune_coast",
        #         "Sandy beach plain": "sandy_beach_plain",
        #         "Estuary inlet": "coastal_inlet",
        #         "Tidal flat's, including marshes, mangroves and sabkha's.": "coastal_wetlands",
        #         "Coastal plain without built-up areas": "coastal_sediment_plain",
        #         "Coastal plain with built-up area": "coastal_sediment_plain",
        #         "Coastal bedrock plain": "coastal_bedrock_plain",
        #         "enh:Coastal bedrock plain with built-up area": "coastal_bedrock_plain",
        #     },
        # "is_built_environment": {True: "true", False: "false"},
        # "has_defense": {"yes": "true", "no": "false"},
    }

    manager.process_records(key_mapping, value_mapping, new_prefix="labels")
