import datetime
import json
import logging

import fsspec

logger = logging.getLogger(__name__)


class CRUDManager:
    def __init__(self, container_name, prefix, storage_options):
        self.container_name = container_name
        self.prefix = prefix
        self.storage_options = storage_options

        # Construct the base path for the storage location
        self.base_path = f"az://{self.container_name}/{self.prefix}/"

    def generate_filename(self, record: dict) -> str:
        raise NotImplementedError("Subclasses must implement this method.")

    def _get_storage_path(self, record_name: str) -> str:
        """Helper method to construct the full path for a record."""
        return f"{self.base_path}{record_name}"

    def save_record(self, record: dict):
        """Saves a record to the Azure storage backend."""
        record_name = self.generate_filename(record)
        record_json = json.dumps(record, indent=4)
        full_path = self._get_storage_path(record_name)

        with fsspec.open(full_path, mode="w", **self.storage_options) as f:
            f.write(record_json)
        logging.info(f"Saved record: {full_path}")

    def create_record(self, record: dict):
        """Creates a new record and saves it."""
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        record["timestamp"] = timestamp
        self.save_record(record)

    def read_record(self, record_name: str) -> dict:
        """Reads a record from the Azure storage backend."""
        full_path = self._get_storage_path(record_name)

        with fsspec.open(full_path, mode="r", **self.storage_options) as f:
            record_json = f.read()

        return json.loads(record_json)

    def update_record(self, record_name: str, updated_data: dict):
        """Updates an existing record in the Azure storage backend."""
        record = self.read_record(record_name)
        record.update(updated_data)
        self.save_record(record)

    def delete_record(self, record_name: str):
        """Deletes a record from the Azure storage backend."""
        full_path = self._get_storage_path(record_name)
        fs = fsspec.filesystem("az", **self.storage_options)
        fs.rm(full_path)
        logging.info(f"Deleted record: {full_path}")
