import datetime
from abc import abstractmethod
from typing import (
    Any,
    Dict,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

import geopandas as gpd
import msgspec
import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

ShoreType = Literal[
    "sandy_gravel_or_small_boulder_sediments",
    "muddy_sediments",
    "rocky_shore_platform_or_large_boulders",
    "no_sediment_or_shore_platform",
    "ice_or_tundra",
]
CoastalType = Literal[
    "cliffed_or_steep",
    "moderately_sloped",
    "bedrock_plain",
    "sediment_plain",
    "dune",
    "wetland",
    "coral",
    "inlet",
    "engineered_structures",
]
LandformType = Literal[
    "mainland_coast",
    "estuary",
    "barrier_island",
    "barrier",
    "pocket_beach",
    "spit",
    "enh:bay",
]
IsBuiltEnvironment = Literal["true", "false"]
HasDefense = Literal["true", "false"]


def encode_custom(obj):
    """Encode custom data types for serialization."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(
        obj,
        (
            GeometryCollection,
            LineString,
            Point,
            Polygon,
            MultiPolygon,
            MultiPoint,
            MultiLineString,
        ),
    ):
        return obj.wkt
    raise TypeError(f"Type {type(obj)} not supported")


def decode_custom(type, obj):
    """Decode custom data types for deserialization."""
    if type is datetime.datetime:
        return datetime.datetime.fromisoformat(obj)
    elif type in {GeometryCollection, LineString, Point}:
        return wkt.loads(obj)
    return obj


class BaseModel(
    msgspec.Struct,
    kw_only=True,  # Set all fields as keyword-only to avoid ordering issues
    tag=True,
    tag_field="type",
    dict=True,
    omit_defaults=True,
    repr_omit_defaults=True,
):
    @property
    def __defined_struct_fields__(self) -> Tuple[str, ...]:
        """Return tuple of fields explicitly defined with non-default values."""
        defined_fields = [
            field.name
            for field in msgspec.structs.fields(self)
            if getattr(self, field.name, None) != field.default
        ]
        return tuple(defined_fields)

    @property
    def __field_types__(self) -> Dict[str, Type]:
        """Return a dictionary of field names and their types."""
        field_types = {}
        for field in msgspec.structs.fields(self):
            base_type = get_args(field.type)[0] if get_args(field.type) else field.type
            if field.name in self.__defined_struct_fields__:
                field_types[field.name] = base_type
        return field_types

    @classmethod
    def null(cls) -> "BaseModel":
        """Create an instance with null values for each field."""
        null_values = {}
        for field in msgspec.structs.fields(cls):
            field_type = field.type
            origin_type = get_origin(
                field_type
            )  # Detect if it's a Literal, Union, etc.

            if origin_type is Literal:
                # Handle Literal by using str as the base type for string literals
                literal_values = get_args(field_type)
                base_type = type(literal_values[0]) if literal_values else str
            else:
                # For non-literal types, handle as usual
                base_type = (
                    get_args(field_type)[0] if get_args(field_type) else field_type
                )

            # Determine the default null value based on base_type
            if base_type is str:
                null_values[field.name] = ""
            elif base_type in {int, float}:
                null_values[field.name] = np.nan
            elif base_type is bool:
                null_values[field.name] = False
            elif base_type == pd.Timestamp:
                null_values[field.name] = pd.NaT
            elif issubclass(base_type, (GeometryCollection, LineString, Point)):
                null_values[field.name] = GeometryCollection()
            elif issubclass(base_type, BaseModel):
                null_values[field.name] = base_type.null()
            else:
                null_values[field.name] = None

        return cls(**null_values)

    @abstractmethod
    def example(cls) -> "BaseModel":
        """Create an example instance with predefined example values."""
        ...

    def to_dict(self) -> dict:
        """
        Convert instance to a flat dictionary format, excluding fields with values equal to their default values.
        Nested BaseModel instances are flattened. Raises KeyError for key conflicts.
        """
        result = {}

        # Iterate over fields and their metadata
        for field in msgspec.structs.fields(self):
            field_name = field.name
            field_value = getattr(self, field_name, None)
            field_default = field.default

            # Skip fields with default values
            if field_value == field_default:
                continue

            # Flatten nested BaseModel instances
            if isinstance(field_value, BaseModel):
                nested_dict = field_value.to_dict()  # Recursively call to_dict()

                # Check for key conflicts and raise KeyError if any conflict is found
                for nested_key in nested_dict:
                    if nested_key in result:
                        raise KeyError(
                            f"Key conflict detected: '{nested_key}' already exists in the parent dictionary."
                        )

                # Merge nested dictionary into the parent
                result.update(nested_dict)
            else:
                # Add non-nested fields to the result
                result[field_name] = field_value

        return result

    def encode(self) -> bytes:
        """Encode instance as JSON bytes."""
        encoder = msgspec.json.Encoder(enc_hook=encode_custom)
        return encoder.encode(self)

    def decode(self, data: bytes):
        """Decode JSON bytes to an instance."""
        decoder = msgspec.json.Decoder(ModelUnion, dec_hook=decode_custom)
        return decoder.decode(data)

    def to_json(self) -> str:
        """Encode instance as JSON string."""
        return self.encode().decode()

    def to_meta(self) -> Dict[str, Type]:
        """Generate a dictionary with field types for metadata."""
        field_types = {}
        for field_name, field_type in self.__field_types__.items():
            if field_type in {float, int, str, bool, pd.Timestamp}:
                field_types[field_name] = field_type
            elif issubclass(field_type, (GeometryCollection, LineString)):
                field_types[field_name] = object
            elif issubclass(field_type, BaseModel):
                field_types[field_name] = object
            else:
                field_types[field_name] = object
        return field_types

    def empty_frame(self) -> "gpd.GeoDataFrame":
        """Create an empty GeoDataFrame with appropriate column types."""
        column_types = {
            col: (
                float
                if dtype in {float, int}
                else "datetime64[ns]"
                if dtype == pd.Timestamp
                else object
            )
            for col, dtype in self.to_meta().items()
        }
        empty_data = {
            col: pd.Series(dtype=col_type) if col_type != GeometryCollection() else []
            for col, col_type in column_types.items()
        }
        return gpd.GeoDataFrame(empty_data, geometry="geometry", crs="EPSG:4326")

    def to_frame(self) -> "gpd.GeoDataFrame":
        """Convert instance to GeoDataFrame format."""
        data = self.to_dict()
        return gpd.GeoDataFrame([data], geometry="geometry", crs="EPSG:4326")

    @classmethod
    def from_json(cls, json_str: str) -> "BaseModel":
        """Create an instance from JSON string."""
        decoder = msgspec.json.Decoder(cls, dec_hook=decode_custom)
        return decoder.decode(json_str.encode())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseModel":
        """Create an instance from a dictionary."""
        return cls(**data)

    @classmethod
    def from_frame(cls, frame: gpd.GeoDataFrame) -> "BaseModel":
        """Create an instance from a GeoDataFrame row."""
        data = frame.iloc[0].to_dict()
        return cls.from_dict(data)


class Transect(BaseModel):
    transect_id: str
    geometry: LineString
    lon: Optional[float] = None
    lat: Optional[float] = None
    bearing: Optional[float] = None
    osm_coastline_is_closed: Optional[bool] = None
    osm_coastline_length: Optional[int] = None
    utm_epsg: Optional[int] = None
    bbox: Optional[dict[str, float]] = None
    quadkey: Optional[str] = None
    continent: Optional[str] = None
    country: Optional[str] = None
    common_country_name: Optional[str] = None
    common_region_name: Optional[str] = None

    @classmethod
    def example(cls):
        _EXAMPLE_VALUES = {
            "transect_id": "T001",
            "geometry": LineString([(34, 45), (44, 55)]),
            "lon": 40.0,
            "lat": 45.0,
            "bearing": 90.0,
            "osm_coastline_is_closed": False,
            "osm_coastline_length": 1000,
            "utm_epsg": 32633,
            "bbox": dict(xmin=34.0, ymin=44.0, xmax=45.0, ymax=55.0),
            "quadkey": "023112",
            "continent": "Europe",
            "country": "Norway",
            "common_country_name": "Norway",
            "common_region_name": "Scandinavia",
        }
        return cls(**_EXAMPLE_VALUES)


class TypologyTrainSample(BaseModel):
    transect: Transect
    user: str
    uuid: str
    datetime_created: datetime.datetime
    datetime_updated: datetime.datetime
    shore_type: ShoreType
    coastal_type: CoastalType
    landform_type: LandformType
    is_built_environment: IsBuiltEnvironment
    has_defense: HasDefense
    is_challenging: bool
    comment: str
    link: str
    confidence: str
    is_validated: bool

    @classmethod
    def example(cls) -> "TypologyTrainSample":
        _EXAMPLE_VALUES = {
            "transect": Transect.example(),
            "user": "example_user",
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
            "datetime_created": datetime.datetime(2023, 1, 1, 12, 0),
            "datetime_updated": datetime.datetime(2023, 1, 2, 12, 0),
            "shore_type": "rocky_shore_platform_or_large_boulders",
            "coastal_type": "cliffed_or_steep",
            "landform_type": "mainland_coast",
            "is_built_environment": "false",
            "has_defense": "true",
            "is_challenging": False,
            "comment": "Sample comment",
            "link": "https://example.com",
            "confidence": "high",
            "is_validated": True,
        }
        return cls(**_EXAMPLE_VALUES)


class TypologyInferenceSample(BaseModel):
    transect: Transect
    pred_shore_type: ShoreType
    pred_coastal_type: CoastalType
    pred_has_defense: HasDefense
    pred_is_built_environment: IsBuiltEnvironment

    @classmethod
    def example(cls) -> "TypologyInferenceSample":
        _EXAMPLE_VALUES = {
            "transect": Transect.example(),
            "pred_shore_type": "rocky_shore_platform_or_large_boulders",
            "pred_coastal_type": "cliffed_or_steep",
            "pred_has_defense": "true",
            "pred_is_built_environment": "false",
        }
        return cls(**_EXAMPLE_VALUES)


class TypologyTestSample(BaseModel):
    train_sample: TypologyTrainSample
    pred_shore_type: ShoreType
    pred_coastal_type: CoastalType
    pred_has_defense: HasDefense
    pred_is_built_environment: IsBuiltEnvironment

    @classmethod
    def example(cls) -> "TypologyTestSample":
        _EXAMPLE_VALUES = {
            "train_sample": TypologyTrainSample.example(),
            "pred_shore_type": "rocky_shore_platform_or_large_boulders",
            "pred_coastal_type": "cliffed_or_steep",
            "pred_has_defense": "true",
            "pred_is_built_environment": "false",
        }
        return cls(**_EXAMPLE_VALUES)


# Union of all possible classes for decoding
ModelUnion = Union[
    Transect,
    TypologyTrainSample,
    TypologyTestSample,
    TypologyInferenceSample,
]

# Testing instantiation
linestring = LineString([[45, 55], [34, 57]])
bounds = linestring.bounds
bbox = {"minx": bounds[0], "miny": bounds[1], "maxx": bounds[2], "maxy": bounds[3]}
transect = Transect(
    transect_id="a",
    geometry=linestring,
    bearing=40,
    bbox=bbox,
)

transect.to_json()
# NOTE: continue with TypologyTrainSample.null().to_frame()
train_sample = TypologyTrainSample(
    transect=transect,
    user="researcher_1",
    uuid="123e4567-e89b-12d3-a456-426614174000",
    datetime_created=datetime.datetime.now(),
    datetime_updated=datetime.datetime.now(),
    shore_type="sandy_gravel_or_small_boulder_sediments",
    coastal_type="cliffed_or_steep",
    landform_type="mainland_coast",
    is_built_environment="false",
    has_defense="true",
    confidence="high",
    is_validated=True,
    is_challenging=False,
    comment="This is a test comment",
    link="https://example.com",
)

train_sample.to_dict()
TypologyTrainSample.example()

# Transect.null()
TypologyTrainSample.null()
test_sample = TypologyTestSample(
    train_sample=train_sample,
    pred_shore_type="sandy_gravel_or_small_boulder_sediments",
    pred_coastal_type="bedrock_plain",
    pred_has_defense="true",
    pred_is_built_environment="false",
)

inference_sample = TypologyInferenceSample(
    transect=transect,
    pred_shore_type="sandy_gravel_or_small_boulder_sediments",
    pred_coastal_type="bedrock_plain",
    pred_has_defense="true",
    pred_is_built_environment="false",
)
print("done")
