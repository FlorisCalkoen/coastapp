import logging

import fsspec
import geopandas as gpd
import hvplot.pandas  # noqa
import pandas as pd
import panel as pn

from coastapp.crud import CRUDManager
from coastapp.libs import read_records_to_pandas
from coastapp.specification import BaseModel, TypologyTrainSample
from coastapp.style_config import COAST_TYPE_COLORS, SHORE_TYPE_MARKERS

logger = logging.getLogger(__name__)


class LabelledTransectManager(CRUDManager):
    shore_type_markers = SHORE_TYPE_MARKERS
    coast_type_colors = COAST_TYPE_COLORS

    def __init__(self, storage_options, container_name, prefix, user_manager):
        super().__init__(container_name=container_name, storage_options=storage_options)
        self.prefix = prefix
        self.user_manager = user_manager
        self._current_uuid = None
        self._df = None
        self._test_df = None

        self._load_test_layers()

        # Set up a watcher on the selected user parameter to trigger updates
        self.user_manager.selected_user.param.watch(
            self._on_selected_user_change, "value"
        )
        self.test_layer_select = pn.widgets.Select(
            options=list(self.test_layer_options.keys())
        )

        self.test_layer_select.param.watch(self._fetch_test_predictions, "value")

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
        )

        return _user_df

    @property
    def test_df(self) -> gpd.GeoDataFrame:
        """Get the test DataFrame."""
        if self._test_df is None:
            self._fetch_test_predictions()
        return self._test_df

    @property
    def current_uuid(self) -> str:
        """Get the current index for navigation."""
        if self._current_uuid is None:
            self._current_uuid = self.user_df.iloc[-1].uuid
        return self._current_uuid

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
        self.test_layer_options = {
            f.split("/")[-1].replace(".parquet", ""): f for f in files
        }
        return self.test_layer_options

    def _fetch_test_predictions(self) -> gpd.GeoDataFrame:
        """Load the test predictions from the selected parquet file."""

        self.test_layer_select.value
        fs = fsspec.filesystem("az", **self.storage_options)
        with fs.open(self.test_layer_options[self.test_layer_select.value]) as f:
            _test_df = gpd.read_parquet(f)
        _test_df = _test_df.dropna(subset="user").reset_index(drop=True)

        # Add color and symbol mapping to the dataframe
        _test_df["coast_color"] = _test_df["pred_coastal_type"].map(
            self.coast_type_colors
        )
        _test_df["shore_marker"] = _test_df["pred_shore_type"].map(
            self.shore_type_markers
        )

        self._test_df = _test_df
        return self._test_df

    def add_record(self, new_record_df: BaseModel) -> None:
        """Add a new record to the in-memory dataframe and update the user_df."""
        new_record_df = new_record_df.to_frame()
        try:
            self._df = pd.concat(
                [self.df, new_record_df], ignore_index=True
            ).reset_index(drop=True)
            logger.info("Record successfully added and dataframe index reset.")
        except Exception as e:
            logger.error(f"Failed to append the new record: {e}")
            raise

    def get_next_record(self) -> dict | None:
        """Get the next record for the current user based on the current index."""

        current_index = self.user_df.index[self.user_df["uuid"] == self.current_uuid][0]
        next_index = current_index + 1
        if next_index >= len(self.user_df):
            next_index = 0

        next_record = self.user_df.iloc[next_index]
        self._current_uuid = next_record.uuid.item()
        try:
            record = TypologyTrainSample.from_frame(next_record).to_dict()
            return record
        except Exception:
            logger.warning(
                f"No records found for user: {self.user_manager.selected_user.value}"
            )
            return None

    def get_previous_record(self) -> BaseModel | None:
        """Get the previous record for the current user based on the current index."""
        current_index = self.user_df.index[self.user_df["uuid"] == self.current_uuid]
        previous_index = current_index - 1
        if previous_index == -1:
            previous_index = len(self.user_df) - 1

        previous_record = self.user_df.iloc[[previous_index]]
        self._current_uuid = previous_record.uuid.item()
        try:
            record = TypologyTrainSample.from_frame(previous_record)
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
