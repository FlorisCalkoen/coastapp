import logging

import fsspec
import geopandas as gpd
import hvplot.pandas  # noqa
import pandas as pd
from crud import CRUDManager

logger = logging.getLogger(__name__)


class LabelledTransectManager(CRUDManager):
    def __init__(self, storage_options, container_name, prefix):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix
        self.df = None  # DataFrame will be lazily loaded

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for transect storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """This method is inherited from CRUDManager, but we're not writing to cloud storage."""
        return "not_used.json"

    def load(self):
        """Load all labelled transects lazily from storage into a GeoPandas dataframe."""
        if self.df is not None:
            return self.df  # If already loaded, return the dataframe

        fs = fsspec.filesystem("az", **self.storage_options)
        labelled_files = fs.glob(f"{self.base_uri}/*.json")

        all_records = []
        for file in labelled_files:
            try:
                record = self.read_record(
                    file.split("/")[-1]
                )  # Read record from storage
                all_records.append(record)
            except Exception as e:
                logger.warning(f"Failed to load record from {file}: {e}")

        if all_records:
            # Convert to Pandas DataFrame
            df = pd.DataFrame(all_records)

            # Ensure timestamps are in datetime format
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Sort and drop duplicates, keeping only the latest per user and transect_id
            df = df.sort_values(by="timestamp").drop_duplicates(
                subset=["user", "transect_id"], keep="last"
            )

            # Convert to GeoPandas DataFrame
            self.df = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs="EPSG:4326"
            )

        else:
            self.df = gpd.GeoDataFrame(
                columns=["user", "transect_id", "timestamp", "lon", "lat", "geometry"]
            )

        return self.df

    def add_record(self, record: dict):
        """Add a new record to the in-memory dataframe."""
        if self.df is None:
            self.load()  # Load the data if it's not already loaded

        # Convert the new record to a GeoPandas DataFrame
        record_df = pd.DataFrame([record])
        record_df["timestamp"] = pd.to_datetime(record_df["timestamp"])

        # Convert to GeoPandas DataFrame
        record_gdf = gpd.GeoDataFrame(
            record_df,
            geometry=gpd.points_from_xy(record_df["lon"], record_df["lat"]),
            crs="EPSG:4326",
        )

        # Check if a record with the same user and transect_id already exists
        existing_index = self.df[
            (self.df["user"] == record["user"])
            & (self.df["transect_id"] == record["transect_id"])
        ].index

        if not existing_index.empty:
            # Replace the existing record
            self.df.drop(existing_index, inplace=True)

        # Append the new record
        self.df = pd.concat([self.df, record_gdf], ignore_index=True)

    def get_latest_records(self):
        """Return the current in-memory dataframe of latest labelled transects."""
        if self.df is None:
            self.load()  # Ensure the data is loaded before returning it
        return self.df

    def plot_labelled_transects(self):
        df = self.get_latest_records()

        plot = df[["geometry"]].hvplot(
            geo=True, color="green", responsive=True, size=10
        )
        return plot
