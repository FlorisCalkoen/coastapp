import datetime
import json
import logging
import os
import uuid
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
            "datetime_created", datetime.datetime.now(datetime.UTC).isoformat()
        )

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

    def filter_records(self, records: list[dict]) -> list[dict]:
        """
        Filters the list of records to keep the latest record per user per transect_id.

        Args:
            records (list[dict]): A list of records, each record is a dictionary containing 'user', 'transect_id',
                                'datetime_created', 'datetime_updated', and other fields.

        Returns:
            list[dict]: A list of records containing the latest record per user per transect_id.
        """

        # Ensure datetime fields are in correct format and sort the records
        for record in records:
            record["datetime_created"] = datetime.datetime.fromisoformat(
                record["datetime_created"]
            )
            record["datetime_updated"] = datetime.datetime.fromisoformat(
                record["datetime_updated"]
            )

        # Sort the list of records by user, transect_id, datetime_created, and datetime_updated
        sorted_records = sorted(
            records,
            key=lambda r: (
                r["user"],
                r["transect_id"],
                r["datetime_created"],
                r["datetime_updated"],
            ),
        )

        # Create a dictionary to store the latest record per user and transect_id
        latest_records = {}

        for record in sorted_records:
            key = (record["user"], record["transect_id"])

            # If the key doesn't exist or the current record is newer, update the dictionary
            if key not in latest_records:
                latest_records[key] = record
            else:
                existing_record = latest_records[key]
                if record["datetime_created"] > existing_record["datetime_created"] or (
                    record["datetime_created"] == existing_record["datetime_created"]
                    and record["datetime_updated"] > existing_record["datetime_updated"]
                ):
                    latest_records[key] = record

        # Return the latest records as a list
        return list(latest_records.values())

    def process_filter_records(self, new_prefix: str | None = None):
        """
        Reads all records from cloud storage, filters the latest record per user and transect_id,
        and saves the filtered records back to cloud storage.
        """
        # File storage and prefix setup
        prefix_to_use = new_prefix if new_prefix else self.get_prefix
        fs = fsspec.filesystem("az", **self.storage_options)

        # Load all records from cloud storage
        labelled_files = fs.glob(f"{self.base_uri}/*.json")
        all_records = []

        for file in labelled_files:
            record_name = file.split("/")[-1]
            try:
                # Read the original record and append it to the list
                record = self.read_record(record_name)
                all_records.append(record)
            except Exception as e:
                logger.error(f"Failed to read record {record_name}: {e}", exc_info=True)

        # If no records found, return early
        if not all_records:
            logger.info("No records found in cloud storage.")
            return

        # Filter records to get the latest per user and transect_id
        filtered_records = self.filter_records(all_records)

        # Save the filtered records back to cloud storage
        for record in filtered_records:
            try:
                # Define the file name for the record
                record_name = self.generate_filename(record)

                # Ensure proper order in saving the fields
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

                # Ensure the record is saved in the correct order
                ordered_record = {k: record.get(k) for k in sort_order if k in record}
                ordered_record["datetime_created"] = ordered_record[
                    "datetime_created"
                ].isoformat()
                ordered_record["datetime_updated"] = ordered_record[
                    "datetime_updated"
                ].isoformat()

                # Save the filtered record with the correct prefix
                self.save_record_with_prefix(ordered_record, record_name, prefix_to_use)
                logger.info(f"Record {record_name} saved successfully.")

            except Exception as e:
                logger.error(f"Failed to save record {record_name}: {e}", exc_info=True)

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

    def process_add_uuid(self, new_prefix: str | None = None):
        """
        Reads all records from cloud storage, filters the latest record per user and transect_id,
        and saves the filtered records back to cloud storage.
        """
        # File storage and prefix setup
        prefix_to_use = new_prefix if new_prefix else self.get_prefix
        fs = fsspec.filesystem("az", **self.storage_options)

        # Load all records from cloud storage
        labelled_files = fs.glob(f"{self.base_uri}/*.json")
        all_records = []

        for file in labelled_files:
            record_name = file.split("/")[-1]
            try:
                # Read the original record and append it to the list
                record = self.read_record(record_name)
                all_records.append(record)
            except Exception as e:
                logger.error(f"Failed to read record {record_name}: {e}", exc_info=True)

        # If no records found, return early
        if not all_records:
            logger.info("No records found in cloud storage.")
            return

        # Filter records to get the latest per user and transect_id
        filtered_records = self.filter_records(all_records)

        # ve the filtered records back to cloud storage
        for record in filtered_records:
            try:
                record["uuid"] = uuid.uuid4().hex[:12]
                # Define the file name for the record
                record_name = self.generate_filename(record)

                # Ensure proper order in saving the fields
                sort_order = [
                    "uuid",
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

                # Ensure the record is saved in the correct order
                ordered_record = {k: record.get(k) for k in sort_order if k in record}

                # Cast datetime objects to isoformat
                ordered_record["datetime_created"] = ordered_record[
                    "datetime_created"
                ].isoformat()
                ordered_record["datetime_updated"] = ordered_record[
                    "datetime_updated"
                ].isoformat()

                # Save the filtered record with the correct prefix
                self.save_record_with_prefix(ordered_record, record_name, prefix_to_use)
                logger.info(f"Record {record_name} saved successfully.")

            except Exception as e:
                logger.error(f"Failed to save record {record_name}: {e}", exc_info=True)

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
        # "timestamp": "datetime_created",  # Renamed to "time"
    }

    value_mapping = {
        #     "shore_type": {
        #         "Sandy, gravel or small boulder sediments": "sandy_gravel_or_small_boulder_sediments",
        #         "Muddy sediments": "muddy_sediments",
        #         "Rocky shore platform or large boulders": "rocky_shore_platform_or_large_boulders",
        #         "Ice/tundra": "ice_or_tundra",
        #         "No sediment or shore platform": "no_sediment_or_shore_platform",
        #     },
        "coastal_type": {
            "cliffed_or_steep_coasts": "cliffed_or_steep",
            "moderately_sloped_coasts": "moderately_sloped",
            "coastal_bedrock_plain": "bedrock_plain",
            "coastal_sediment_plain": "sediment_plain",
            "dune_coast": "dune",
            "coastal_wetlands": "wetland",
            "coral_coast": "coral",
            "coastal_inlet": "inlet",
            "engineered_coastal_structures": "engineered_structures",
        },
        # "is_built_environment": {True: "true", False: "false"},
        # "has_defense": {"yes": "true", "no": "false"},
    }

    # manager.process_records(
    #     key_mapping=key_mapping, value_mapping=value_mapping, new_prefix="labels"
    # )

    manager.process_add_uuid(new_prefix="labels")
