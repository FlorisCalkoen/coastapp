import logging
from typing import Literal

import fsspec
import geopandas as gpd
import hvplot.pandas  # noqa
import pandas as pd
import panel as pn

from coastapp.crud import CRUDManager
from coastapp.libs import read_records_to_pandas
from coastapp.shared_state import shared_state
from coastapp.specification import (
    BaseModel,
    Transect,
    TypologyTestSample,
    TypologyTrainSample,
)
from coastapp.style_config import COAST_TYPE_COLORS, SHORE_TYPE_MARKERS

logger = logging.getLogger(__name__)


class LabelledTransectManager(CRUDManager):
    shared_state = shared_state
    shore_type_markers = SHORE_TYPE_MARKERS
    coastal_type_colors = COAST_TYPE_COLORS

    def __init__(self, storage_options, container_name, prefix, user_manager):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix
        self.user_manager = user_manager
        self._current_uuid = None
        self._current_test_uuid = None
        self._current_benchmark_uuid = None
        self._df = None
        self._test_df = None
        self._benchmark_df = None

        self._load_test_layers()
        self._load_benchmark_layers()

        # Set up a watcher on the selected user parameter to trigger updates
        self.user_manager.selected_user.param.watch(
            self._on_selected_user_change, "value"
        )
        self.test_layer_select = pn.widgets.Select(
            options=list(self.test_layer_options.keys())
        )

        self.test_layer_select.param.watch(self._fetch_test_predictions, "value")

        self.benchmark_layer_select = pn.widgets.Select(
            options=list(self.benchmark_layer_options.keys())
        )
        self.benchmark_layer_select.param.watch(self._fetch_benchmark_samples, "value")

    @property
    def get_prefix(self) -> str:
        """Defines the prefix for classification storage."""
        return self.prefix

    @property
    def df(self) -> gpd.GeoDataFrame:
        """Lazy-loaded property for the main DataFrame."""
        if self._df is None:
            self.load()
        return self._df

    @property
    def user_df(self) -> gpd.GeoDataFrame:
        """Get the user-specific DataFrame."""
        _user_df = self.df[self.df["user"] == self.user_manager.selected_user.value]

        _user_df = (
            _user_df.sort_values(["datetime_created", "datetime_updated"])
            .groupby("transect_id")
            .tail(1)
            .reset_index(drop=True)
        )

        return _user_df

    @property
    def test_df(self) -> gpd.GeoDataFrame:
        """Get the test DataFrame."""
        if self._test_df is None:
            test_df = self._fetch_test_predictions()
            self._test_df = test_df
        return self._filter_test_df(self._test_df)

    @property
    def benchmark_df(self) -> gpd.GeoDataFrame:
        """Get the test DataFrame."""
        if self._benchmark_df is None:
            benchmark_df = self._fetch_benchmark_samples()
            self._benchmark_df = benchmark_df
        return self._benchmark_df

    @property
    def current_uuid(self) -> str:
        """Get the current index for navigation."""
        if self._current_uuid is None:
            self._current_uuid = self.user_df.iloc[-1].uuid
        return self._current_uuid

    @property
    def current_test_uuid(self) -> str:
        """Get the current index for navigation."""
        if self._current_test_uuid is None:
            self._current_test_uuid = self.test_df.iloc[-1].uuid
        return self._current_test_uuid

    @property
    def current_benchmark_uuid(self) -> str:
        """Get the current index for navigation."""
        if self._current_benchmark_uuid is None:
            self._current_benchmark_uuid = self.benchmark_df.iloc[-1].uuid
        return self._current_benchmark_uuid

    def load(self) -> gpd.GeoDataFrame:
        """Load all labelled transects from storage into a GeoPandas dataframe."""
        container = f"{self.base_uri}/*.json"
        self._df = read_records_to_pandas(BaseModel, container, self.storage_options)
        return self._df

    def reload(self) -> gpd.GeoDataFrame:
        """Forces a reload of the main DataFrame."""
        self._df = self.load()
        return self.df

    def _on_selected_user_change(self, event) -> None:
        new_user = event.new
        logger.info(f"User changed to {new_user}, updating user_df.")
        self._current_uuid = None

    def _load_test_layers(self) -> dict:
        TEST_PREDICTIONS_PREFIX = "az://typology/test/*.parquet"
        fs = fsspec.filesystem("az", **self.storage_options)
        files = fs.glob(TEST_PREDICTIONS_PREFIX)
        files.reverse()  # Reverse the order to show the latest files first
        self.test_layer_options = {
            f.split("/")[-1].replace(".parquet", ""): f for f in files
        }
        return self.test_layer_options

    def _load_benchmark_layers(self) -> dict:
        BENCHMARK_PREFIX = "az://typology/benchmark/*.parquet"
        fs = fsspec.filesystem("az", **self.storage_options)
        files = fs.glob(BENCHMARK_PREFIX)
        self.benchmark_layer_options = {
            f.split("/")[-1].replace(".parquet", ""): f for f in files
        }
        return self.benchmark_layer_options

    def _fetch_test_predictions(self, event=None) -> gpd.GeoDataFrame:
        """Load the test predictions from the selected parquet file."""

        # self.test_layer_select.value
        fs = fsspec.filesystem("az", **self.storage_options)
        with fs.open(self.test_layer_options[self.test_layer_select.value]) as f:
            _test_df = gpd.read_parquet(f)
        _test_df = _test_df.dropna(subset="user").reset_index(drop=True)

        # Add color and symbol mapping to the dataframe
        _test_df["coastal_type_color"] = _test_df["pred_coastal_type"].map(
            self.coastal_type_colors
        )
        _test_df["shore_type_marker"] = _test_df["pred_shore_type"].map(
            self.shore_type_markers
        )

        self._test_df = _test_df
        return self._test_df

    def _fetch_benchmark_samples(self, event=None) -> gpd.GeoDataFrame:
        """Load the test predictions from the selected parquet file."""

        # self.test_layer_select.value
        fs = fsspec.filesystem("az", **self.storage_options)
        pathlike = self.benchmark_layer_options[self.benchmark_layer_select.value]
        with fs.open(pathlike) as f:
            _df = gpd.read_parquet(f)

        _df = _df.reset_index(drop=True)

        self._current_benchmark_uuid = None
        self._benchmark_df = _df
        return self._benchmark_df

    def _filter_test_df(self, test_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters the test_df based on application-specific widget states.

        Args:
            test_df (pd.DataFrame): The DataFrame to filter.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        # Get the latest user-specific records
        df = self.df

        # Merge test_df with user_df to update 'is_validated' and 'confidence'
        updated_test_df = test_df.merge(
            df[["transect_id", "user", "is_validated", "confidence"]],
            on=["transect_id", "user"],
            how="left",
            suffixes=("", "_user"),
        )

        # Overwrite test_df columns with user_df values where available
        updated_test_df["is_validated"] = updated_test_df[
            "is_validated_user"
        ].combine_first(updated_test_df["is_validated"])
        updated_test_df["confidence"] = updated_test_df[
            "confidence_user"
        ].combine_first(updated_test_df["confidence"])

        # Drop the merged columns from user_df
        updated_test_df = updated_test_df.drop(
            columns=["is_validated_user", "confidence_user"]
        )

        # Apply filtering for incorrect predictions
        if self.shared_state.only_use_incorrect:
            updated_test_df = updated_test_df[
                (updated_test_df["shore_type"] != updated_test_df["pred_shore_type"])
                | (
                    updated_test_df["coastal_type"]
                    != updated_test_df["pred_coastal_type"]
                )
            ]

        # Filter for non-validated samples if enabled
        if self.shared_state.only_use_non_validated:
            updated_test_df = updated_test_df[~updated_test_df["is_validated"]]

        # Filter by confidence levels
        confidence_hierarchy = {
            "low": ["low", "medium", "high"],
            "medium": ["medium", "high"],
            "high": ["high"],
        }
        confidence_level = self.shared_state.confidence_filter_slider.value
        valid_confidences = confidence_hierarchy[confidence_level]
        updated_test_df = updated_test_df[
            updated_test_df["confidence"].isin(valid_confidences)
        ]

        return updated_test_df.reset_index(drop=True)

    def add_record(self, new_record: TypologyTrainSample) -> None:
        """Add a new record to the in-memory dataframe and update the user_df."""
        new_record_df = new_record.to_frame()
        try:
            self._df = pd.concat(
                [self.df, new_record_df], ignore_index=True
            ).reset_index(drop=True)
            logger.info("Record successfully added and dataframe index reset.")
        except Exception as e:
            logger.error(f"Failed to append the new record: {e}")
            raise

    def get_next_record(
        self, dataframe: Literal["user_df", "test_df", "benchmark_df"]
    ) -> BaseModel | None:
        """
        Get the next record from the specified dataframe (user_df or test_df).

        Args:
            dataframe (Literal["user_df", "test_df"]): The dataframe to query.

        Returns:
            BaseModel | None: The next record as a BaseModel, or None if no record is found.
        """
        # Retrieve the dataframe based on the argument

        df = getattr(self, dataframe)

        # Determine the correct UUID to use
        if dataframe == "user_df":
            current_uuid = self.current_uuid
        elif dataframe == "test_df":
            current_uuid = self.current_test_uuid
        elif dataframe == "benchmark_df":
            current_uuid = self.current_benchmark_uuid
        else:
            raise ValueError(f"Invalid dataframe specified: {dataframe}")

        # Check if the dataframe is empty or the current UUID is invalid
        if df.empty or current_uuid not in df["uuid"].values:
            logger.warning(f"No records available in {dataframe} or invalid UUID.")
            return None

        try:
            # Get the current index and compute the next index
            current_index = int(df.index[df["uuid"] == current_uuid][0])
            next_index = (current_index + 1) % len(df)

            # Retrieve the next record
            next_record = df.iloc[[next_index]]

            # Update the appropriate UUID
            if dataframe == "user_df":
                self._current_uuid = next_record.uuid.item()
            elif dataframe == "test_df":
                self._current_test_uuid = next_record.uuid.item()
            elif dataframe == "benchmark_df":
                self._current_benchmark_uuid = next_record.uuid.item()

            # Convert the record to the appropriate BaseModel
            if dataframe == "user_df":
                record = TypologyTrainSample.from_frame(next_record)
            elif dataframe == "test_df":
                train_sample = TypologyTrainSample.from_frame(next_record)
                record = TypologyTestSample(
                    train_sample=train_sample,
                    pred_shore_type=next_record.pred_shore_type.item(),
                    pred_coastal_type=next_record.pred_coastal_type.item(),
                    pred_has_defense=next_record.pred_has_defense.item(),
                    pred_is_built_environment=next_record.pred_is_built_environment.item(),
                )
                return record
            elif dataframe == "benchmark_df":
                record = Transect.from_frame(next_record)
            else:
                raise ValueError(f"Invalid dataframe specified: {dataframe}")

            return record

        except Exception as e:
            logger.warning(
                f"Error retrieving next record in {dataframe} for user: {self.user_manager.selected_user.value}. Error: {e}"
            )
            return None

    def get_previous_record(
        self, dataframe: Literal["user_df", "test_df"]
    ) -> BaseModel | None:
        """
        Get the previous record from the specified dataframe (user_df or test_df).

        Args:
            dataframe (str): The dataframe to query. Options are 'user_df' or 'test_df'.

        Returns:
            BaseModel | None: The next record as a BaseModel, or None if no record found.
        """

        df = getattr(self, dataframe)

        # Determine the correct UUID to use
        if dataframe == "user_df":
            current_uuid = self.current_uuid
        elif dataframe == "test_df":
            current_uuid = self.current_test_uuid
        elif dataframe == "benchmark_df":
            current_uuid = self.current_benchmark_uuid
        else:
            raise ValueError(f"Invalid dataframe specified: {dataframe}")

        # Check if the dataframe is empty or the current UUID is invalid
        if df.empty or current_uuid not in df["uuid"].values:
            logger.warning(f"No records available in {dataframe} or invalid UUID.")
            return None

        try:
            # Get the current index and compute the previous index
            current_index = int(df.index[df["uuid"] == current_uuid][0])
            previous_index = current_index - 1

            # Wrap around to the end of the dataframe if at the start
            if previous_index == -1:
                previous_index = len(df) - 1

            # Retrieve the previous record
            previous_record = df.iloc[[previous_index]]

            # Update the appropriate UUID
            if dataframe == "user_df":
                self._current_uuid = previous_record.uuid.item()

            elif dataframe == "test_df":
                self._current_test_uuid = previous_record.uuid.item()

            elif dataframe == "benchmark_df":
                self._current_benchmark_uuid = previous_record.uuid.item()

            # Convert the record to the appropriate BaseModel
            if dataframe == "user_df":
                record = TypologyTrainSample.from_frame(previous_record)

            elif dataframe == "test_df":
                train_sample = TypologyTrainSample.from_frame(previous_record)
                record = TypologyTestSample(
                    train_sample=train_sample,
                    pred_shore_type=previous_record.pred_shore_type.item(),
                    pred_coastal_type=previous_record.pred_coastal_type.item(),
                    pred_has_defense=previous_record.pred_has_defense.item(),
                    pred_is_built_environment=previous_record.pred_is_built_environment.item(),
                )
                return record

            elif dataframe == "benchmark_df":
                record = Transect.from_frame(previous_record)

            else:
                raise ValueError(f"Invalid dataframe specified: {dataframe}")

            return record

        except Exception:
            logger.warning(
                f"No records found for user: {self.user_manager.selected_user.value}"
            )
            return None

    def fetch_record_by_uuid(self, uuid) -> BaseModel | None:
        """Fetches record by UUID, loading data if not already loaded."""
        # Search for UUID in the loaded data
        record = self.df[self.df["uuid"] == uuid]

        if not record.empty:
            return TypologyTrainSample.from_frame(record)

        return None

    def generate_filename(self, record: dict) -> str:
        """This method is inherited from CRUDManager, but we're not writing to cloud storage."""
        return "not_used.json"
