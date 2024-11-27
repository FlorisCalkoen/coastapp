# TODO: import from coastpy
import logging

import fsspec
import geopandas as gpd
import pandas as pd
from fsspec.utils import get_protocol

from coastapp.specification import BaseModel

# Set up logging
logger = logging.getLogger(__name__)


def resolve_path(pathlike: str, fs: fsspec.AbstractFileSystem) -> str:
    """
    Resolve the path for either local or cloud storage using the provided filesystem object.

    Args:
        pathlike (str): Path to the file or pattern.
        fs (fsspec.AbstractFileSystem): Filesystem object for storage access.

    Returns:
        str: Resolved path (signed URL for cloud storage or the local path).
    """
    protocol = fs.protocol
    if protocol in {"az", "abfs", "s3", "gcs"}:  # Cloud storage protocols
        storage_options = fs.storage_options
        account_name = storage_options.get("account_name")
        sas_token = storage_options.get(
            "sas_token", storage_options.get("credential", "")
        )
        if not account_name:
            raise ValueError(
                "Missing 'account_name' in storage options for cloud storage."
            )
        base_url = f"https://{account_name}.blob.core.windows.net"
        return (
            f"{base_url}/{pathlike}?{sas_token}"
            if sas_token
            else f"{base_url}/{pathlike}"
        )
    return pathlike


def write_record(
    record: BaseModel,
    pathlike: str,
    fs: fsspec.AbstractFileSystem,
) -> None:
    """
    Read a single record from cloud storage and parse it into the specified model.

    Args:
        pathlike (str): Path to the specific record inside the container.
        model (Type[BaseModel]): The model class to decode the record into.
        fs (fsspec.AbstractFileSystem): Filesystem object for storage access.

    Returns:
        BaseModel: Parsed instance of the specified data model.
    """
    try:
        with fs.open(pathlike, mode="w") as f:
            f.write(record.to_json())
    except Exception as e:
        logger.error(f"Failed to write or encode record at {pathlike}: {e}")
        msg = f"Error writing record at {pathlike}: {e}"
        raise ValueError(msg) from e


def read_record(
    pathlike: str,
    model: type[BaseModel],
    fs: fsspec.AbstractFileSystem,
) -> BaseModel:
    """
    Read a single record from cloud storage and parse it into the specified model.

    Args:
        pathlike (str): Path to the specific record inside the container.
        model (Type[BaseModel]): The model class to decode the record into.
        fs (fsspec.AbstractFileSystem): Filesystem object for storage access.

    Returns:
        BaseModel: Parsed instance of the specified data model.
    """
    pathlike = resolve_path(pathlike, fs=fs)
    try:
        with fsspec.open(pathlike, mode="r") as f:
            return model().decode(f.read())
    except Exception as e:
        logger.error(f"Failed to read or decode record at {pathlike}: {e}")
        msg = f"Error reading record at {pathlike}: {e}"
        raise ValueError(msg) from e


def read_records_to_pandas(
    model: type[BaseModel],
    container: str,
    storage_options: dict,
) -> "gpd.GeoDataFrame":
    """
    Read and process all records from cloud storage.

    Args:
        model (Type[BaseModel]): The model class to decode the records into.
        container (str): pathlike to the container containing the records - can be pattern (az://typology/labels/*.json).
        storage_options (dict): Dictionary containing storage configurations (e.g., account_name, sas_token).

    Returns:
        gpd.GeoDataFrame: Processed GeoDataFrame with all stored records.
    """
    protocol = get_protocol(container)
    fs = fsspec.filesystem(protocol, **storage_options)
    paths = fs.glob(container)

    records = []
    for p in paths:
        try:
            record = read_record(p, model, fs=fs)
            records.append(record)
        except Exception as e:
            logger.warning(f"Failed to load record from {p}: {e}")

    if not records:
        msg = "No valid records found."
        raise ValueError(msg)

    return pd.concat([r.to_frame() for r in records])
