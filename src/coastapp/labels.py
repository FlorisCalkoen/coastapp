import logging

import fsspec
import geopandas as gpd
import hvplot.pandas  # noqa
import pandas as pd
from shapely import wkt

from coastapp.crud import CRUDManager

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
        self.user_manager.selected_user.param.watch(
            self._on_selected_user_change, "value"
        )

    def _on_selected_user_change(self, event):
        """Callback triggered when selected_user changes."""
        new_user = event.new
        logger.info(f"User changed to {new_user}, updating user_df.")
        if self.df is not None:
            self.update_user_df(new_user)

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for classification storage."""
        return self.prefix

    def generate_filename(self, record: dict) -> str:
        """This method is inherited from CRUDManager, but we're not writing to cloud storage."""
        return "not_used.json"

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
            df["datetime_created"] = pd.to_datetime(df["datetime_created"])
            df["datetime_updated"] = pd.to_datetime(df["datetime_updated"])
            df["geometry"] = df["geometry"].apply(wkt.loads)
            self.df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        else:
            self.df = gpd.GeoDataFrame(
                columns=[
                    "user",
                    "transect_id",
                    "datetime_created",
                    "datetime_updated",
                    "lon",
                    "lat",
                    "geometry",
                ]
            )

        return self.df

    def extract_user_df(self, user):
        """
        Extract the latest record per transect_id for a given user, sorted by datetime_created.
        """
        # Filter by user
        user_records = self.df[self.df["user"] == user]

        if user_records.empty:
            return gpd.GeoDataFrame()

        # Sort by transect_id, datetime_created, and datetime_updated
        user_records = user_records.sort_values(
            by=["transect_id", "datetime_created", "datetime_updated"],
            ascending=[True, True, True],
        )

        # Keep the latest record for each transect_id
        latest_records = (
            user_records.groupby("transect_id").tail(1).reset_index(drop=True)
        )

        # Sort by datetime_created for iteration
        latest_records = latest_records.sort_values(by="datetime_created").reset_index(
            drop=True
        )

        return latest_records

    def add_record(self, record: dict):
        """Add a new record to the in-memory dataframe and update the user_df."""
        # Ensure that the DataFrame is loaded
        if self.df is None:
            self.load()

        # Create the new record as a DataFrame and convert types
        record_df = pd.DataFrame([record])
        record_df["datetime_created"] = pd.to_datetime(record_df["datetime_created"])
        record_df["datetime_updated"] = pd.to_datetime(record_df["datetime_updated"])
        record_df["geometry"] = record_df["geometry"].apply(wkt.loads)

        # Append the new record to the existing dataframe and reset the index
        try:
            self.df = pd.concat([self.df, record_df], ignore_index=True).reset_index(
                drop=True
            )
            logger.info("Record successfully added and dataframe index reset.")
        except ValueError as e:
            logger.error(f"Failed to append the new record: {e}")
            raise

        # Update the user-specific dataframe and keep the current index
        self.update_user_df(record["user"], preserve_current_index=True)

    def update_user_df(self, user, preserve_current_index=False):
        """
        Update the user-specific dataframe based on the latest records per transect_id.
        Optionally preserves the current index if it's set to True.
        """
        previous_index_transect_id = None
        if (
            preserve_current_index
            and self.current_index is not None
            and not self.user_df.empty
        ):
            previous_index_transect_id = self.user_df.iloc[self.current_index][
                "transect_id"
            ]

        # Update the user_df with the latest records for the user
        self.user_df = self.extract_user_df(user)

        if self.user_df.empty:
            self.current_index = None
        else:
            # Restore the current index to the same transect_id if possible
            if preserve_current_index and previous_index_transect_id is not None:
                matching_indices = self.user_df.index[
                    self.user_df["transect_id"] == previous_index_transect_id
                ]
                if not matching_indices.empty:
                    self.current_index = matching_indices[0]
                else:
                    # Default to the last record
                    self.current_index = len(self.user_df) - 1
            else:
                # Default to the last record
                self.current_index = len(self.user_df) - 1

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

        if isinstance(record["datetime_created"], pd.Timestamp):
            record["datetime_created"] = record[
                "datetime_created"
            ].isoformat()  # Format timestamp to ISO string

        if isinstance(record["datetime_updated"], pd.Timestamp):
            record["datetime_updated"] = record[
                "datetime_updated"
            ].isoformat()  # Format timestamp to ISO string

        # Ensure all required fields are present, fill with None if missing
        required_fields = [
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
