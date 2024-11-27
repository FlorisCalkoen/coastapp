import json
import logging
from abc import ABC, abstractmethod

import fsspec

from coastapp.libs import write_record
from coastapp.specification import TypologyTrainSample
from coastapp.utils import name_typology_record

logger = logging.getLogger(__name__)


class CRUDManager(ABC):
    def __init__(self, container_name, storage_options):
        self.container_name = container_name
        self.storage_options = storage_options
        self.container_base_url = (
            f"https://{storage_options['account_name']}.blob.core.windows.net"
        )
        self.container_base_uri = f"az://{self.container_name}"

    @property
    @abstractmethod
    def get_prefix(self) -> str:
        """Abstract property to define the prefix for storage."""

    @property
    def base_uri(self) -> str:
        """Defines the base path for the Azure Blob Storage (az://) protocol, ensuring no double slashes."""
        prefix = self.get_prefix
        if prefix:
            return f"{self.container_base_uri}/{prefix}"
        return f"{self.container_base_uri}"

    @property
    def base_url(self) -> str:
        """Defines the base URL for the HTTPS access, ensuring no double slashes."""
        prefix = self.get_prefix
        if prefix:
            return f"{self.container_base_url}/{self.container_name}/{prefix}"
        return f"{self.container_base_url}/{self.container_name}"

    @abstractmethod
    def generate_filename(self, record: dict) -> str:
        """Abstract method to generate the filename based on the record data."""

    def _get_storage_path(self, record_name: str) -> str:
        """Helper method to construct the full path for a record using the az:// protocol."""
        return f"{self.base_uri}/{record_name}"

    def _get_signed_url(self, record_name: str) -> str:
        """Constructs the signed HTTPS URL with the SAS token."""
        return f"{self.base_url}/{record_name}?{self.storage_options['sas_token']}"

    def create_record(self, record: TypologyTrainSample):
        """Saves a record to the Azure storage backend using the az:// protocol."""
        if isinstance(record, TypologyTrainSample):
            record_name = name_typology_record(record)
        else:
            raise ValueError("Invalid record type.")
        full_path = self._get_storage_path(record_name)
        fs = fsspec.filesystem("az", **self.storage_options)
        write_record(record, full_path, fs)

    def read_json(self, record_name: str) -> dict:
        """Reads a record from the Azure storage backend using HTTPS."""
        # Open the file using https to avoid issues in Panel apps
        signed_url = self._get_signed_url(record_name)
        with fsspec.open(signed_url, mode="r") as f:
            record = json.load(f)
        return record

    def update_record(self, record_name: str, updated_data: dict):
        """Updates an existing record in the Azure storage backend using the az:// protocol."""
        record = self.read_json(record_name)
        record.update(updated_data)
        self.create_record(record)

    def delete_record(self, record_name: str):
        """Deletes a record from the Azure storage backend using the az:// protocol."""
        full_path = self._get_storage_path(record_name)
        fs = fsspec.filesystem("az", **self.storage_options)
        fs.rm(full_path)
        logger.info(f"Deleted record: {full_path}")
