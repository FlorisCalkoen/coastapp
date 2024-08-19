import logging

import fsspec
import geopandas as gpd
import hvplot.pandas  # noqa
import pandas as pd
from crud import CRUDManager
from shapely import wkt

logger = logging.getLogger(__name__)


class LabelledTransectManager(CRUDManager):

    def __init__(self, storage_options, container_name, prefix, user_manager):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix
        self.df = None  # Full DataFrame for all users
        self.user_df = None  # DataFrame to hold records for the current user
        self.current_index = None  # Tracks the current index for navigation
        self.user_manager = user_manager

        # Set up a watcher on the selected user parameter to trigger updates
        self.user_manager.selected_user.param.watch(self._on_selected_user_change, "value")

    def _on_selected_user_change(self, event):
        """Callback triggered when selected_user changes."""
        new_user = event.new
        logger.info(f"User changed to {new_user}, updating user_df.")
        self.update_user_df(new_user)

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for classification storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """This method is inherited from CRUDManager, but we're not writing to cloud storage."""
        return "not_used.json"

    def format_dataframe(self):
        """Ensure only the latest record per user and transect_id is kept, sorted by timestamp."""
        if self.df is not None and not self.df.empty:
            self.df = self.df.sort_values(by="timestamp").drop_duplicates(
                subset=["user", "transect_id"], keep="last"
            )

    def update_user_df(self, user: str):
        """
        Update the user-specific dataframe based on the current user.
        If self.df is None, return early.
        """
        if self.df is None or user is None:
            return None

        # Filter the dataframe for the given user and sort by timestamp
        self.user_df = (
            self.df[self.df["user"] == user]
            .sort_values(by="timestamp")
            .reset_index(drop=True)
        )

        # Set current_index to the last record (or None if user_df is empty)
        self.current_index = len(self.user_df) - 1 if not self.user_df.empty else None

    def load(self, force_reload=False):
        """
        Load all labelled transects from storage into a GeoPandas dataframe.
        """
        if self.df is not None and not force_reload:
            return self.df

        # Load the data
        fs = fsspec.filesystem("az", **self.storage_options)
        labelled_files = fs.glob(f"{self.base_uri}/*.json")

        all_records = []
        for file in labelled_files:
            try:
                record = self.read_record(file.split("/")[-1])
                all_records.append(record)
            except Exception as e:
                logger.warning(f"Failed to load record from {file}: {e}")

        if all_records:
            df = pd.DataFrame(all_records)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["geometry"] = df["geometry"].apply(wkt.loads)
            self.df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        else:
            self.df = gpd.GeoDataFrame(
                columns=["user", "transect_id", "timestamp", "lon", "lat", "geometry"]
            )

        self.format_dataframe()
        return self.df

    def add_record(self, record: dict):
        """Add a new record to the in-memory dataframe and update the user_df."""
        if self.df is None:
            self.load()

        record_df = pd.DataFrame([record])
        record_df["timestamp"] = pd.to_datetime(record_df["timestamp"])
        record_df["geometry"] = record_df["geometry"].apply(wkt.loads)

        record_gdf = gpd.GeoDataFrame(record_df, geometry="geometry", crs="EPSG:4326")

        # Append the new record
        try:
            self.df = pd.concat([self.df, record_gdf], ignore_index=True)
        except ValueError as e:
            logger.error(f"Failed to append the new record: {e}")
            raise

        # Re-filter the full dataframe to ensure latest records are kept
        self.format_dataframe()

        # Update user_df for the current user and set the current_index to the last record
        self.update_user_df(record["user"])

    def get_next_record(self):
        """Get the next record for the current user based on the current index."""
        self.load()

        if self.user_df is None or self.user_df.empty:
            self.update_user_df(self.user_manager.selected_user.value)

        if self.user_df.empty or self.current_index is None:
            logger.warning(
                f"No records found for user: {self.user_manager.selected_user.value}"
            )
            return None

        # Move to the next record, ensuring we don't exceed the available records
        self.current_index = min(self.current_index + 1, len(self.user_df) - 1)

        next_record = self.user_df.iloc[self.current_index]
        return self.format_record(next_record.to_dict())

    def get_previous_record(self):
        """Get the previous record for the current user based on the current index."""
        self.load()

        if self.user_df is None or self.user_df.empty:
            self.update_user_df(self.user_manager.selected_user.value)

        if self.user_df.empty or self.current_index is None:
            logger.warning(
                f"No records found for user: {self.user_manager.selected_user.value}"
            )
            return None

        # Move to the previous record, ensuring we don't go below the first record
        self.current_index = max(self.current_index - 1, 0)

        previous_record = self.user_df.iloc[self.current_index]
        return self.format_record(previous_record.to_dict())

    def format_record(self, record):
        """Format the record to match the classification schema."""
        record["geometry"] = record["geometry"].wkt  # Convert geometry to WKT format

        if isinstance(record["timestamp"], pd.Timestamp):
            record["timestamp"] = record[
                "timestamp"
            ].isoformat()  # Format timestamp to ISO string

        # Ensure all required fields are present, fill with None if missing
        required_fields = [
            "user",
            "transect_id",
            "lon",
            "lat",
            "geometry",
            "timestamp",
            "shore_fabric",
            "coastal_type",
            "defenses",
            "is_challenging",
            "comment",
            "link",
        ]
        for field in required_fields:
            if field not in record:
                record[field] = None

        return record

    def plot_labelled_transects(self):
        """Plot the labelled transects from the loaded GeoDataFrame."""
        df = self.load()  # Ensure data is loaded

        # Create a copy of the dataframe for plotting as points
        plot_df = df.copy()

        # Convert to points for plotting
        plot_df = gpd.GeoDataFrame(
            plot_df.drop(columns=["geometry"]),
            geometry=gpd.points_from_xy(plot_df["lon"], plot_df["lat"]),
            crs="EPSG:4326",
        )

        plot = plot_df[["geometry"]].hvplot(
            geo=True,
            color="red",
            responsive=True,
            size=50,
            label="Labelled Transects",
            line_color="green",
        )
        return plot
