import datetime
import enum
import re
import unicodedata
import uuid
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
from msgspec import field
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
    "barrier_island",
    "barrier_system",
    "barrier",
    "bay",
    "coral_island",
    "delta",
    "estuary",
    "headland",
    "lagoon",
    "mainland_coast",
    "pocket_beach",
    "spit",
    "tombolo",
    "N/A",
]
IsBuiltEnvironment = Literal["true", "false"]
HasDefense = Literal["true", "false"]

PANDAS_TYPE_MAP = {
    # Primitive Types
    str: "object",
    int: "int64",
    float: "float64",
    bool: "bool",
    datetime.datetime: "datetime64[ns]",
    datetime.date: "datetime64[ns]",
    datetime.time: "object",
    dict: "object",  # Nested structures
    list: "object",  # Arrays
    tuple: "object",  # Arrays
    uuid.UUID: "object",
    # Geometries (Shapely)
    Point: "object",
    LineString: "object",
    Polygon: "object",
    MultiPoint: "object",
    MultiLineString: "object",
    MultiPolygon: "object",
    GeometryCollection: "object",
    # Enums and Fallbacks
    enum.Enum: "object",
    object: "object",  # Catch-all for unsupported types
}

GEOPARQUET_TYPE_MAP = {
    # Primitive Types
    str: "STRING",
    int: "INTEGER",
    float: "DOUBLE",
    bool: "BOOLEAN",
    datetime.datetime: "DATETIME",
    datetime.date: "DATE",
    datetime.time: "TIME",
    dict: "STRUCT",  # Nested structures
    list: "ARRAY",  # Arrays
    tuple: "ARRAY",  # Arrays
    uuid.UUID: "STRING",
    # Geometries (Shapely)
    Point: "GEOMETRY",
    LineString: "GEOMETRY",
    Polygon: "GEOMETRY",
    MultiPoint: "GEOMETRY",
    MultiLineString: "GEOMETRY",
    MultiPolygon: "GEOMETRY",
    GeometryCollection: "GEOMETRY",
    # Enums and Fallbacks
    enum.Enum: "STRING",  # Serialize enums as strings
    object: "STRING",  # Catch-all for unsupported types
}


def custom_schema_hook(typ):
    """Provide JSON schema for custom types."""
    if typ is LineString:
        # Represent LineString as a WKT string
        return {"type": "string", "description": "A WKT representation of a LineString"}
    if typ is dict:  # Example for bbox as dict
        return {
            "type": "object",
            "properties": {
                "xmin": {"type": "number"},
                "ymin": {"type": "number"},
                "xmax": {"type": "number"},
                "ymax": {"type": "number"},
            },
            "required": ["xmin", "ymin", "xmax", "ymax"],
        }
    return None  # For unsupported types, fallback to default behavior


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
    elif type in {
        GeometryCollection,
        LineString,
        Point,
        Polygon,
        MultiPolygon,
        MultiPoint,
        MultiLineString,
    }:
        try:
            return wkt.loads(obj)
        except Exception as e:
            raise ValueError(f"Failed to decode geometry: {e}")
    return obj


class BaseModel(
    msgspec.Struct,
    kw_only=True,  # Set all fields as keyword-only to avoid ordering issues
    tag=str.lower,
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
        """
        Generate a flat dictionary of field names and their types, consistent with `to_dict()`.
        Nested BaseModel fields are flattened, with conflicts raising a KeyError.
        """
        # Start extraction from the current class
        return self._get_field_types(self.__class__)

    @classmethod
    def _get_field_types(cls, struct_cls: Type[msgspec.Struct]) -> Dict[str, Type]:
        """
        Recursively extract field types for a given `msgspec.Struct` class.

        Args:
            struct_cls (Type[msgspec.Struct]): The `msgspec.Struct` class to extract types from.

        Returns:
            Dict[str, Type]: A dictionary of field names and their resolved types.
        """
        field_types = {}

        for field_ in msgspec.structs.fields(struct_cls):
            field_name = field_.name

            # Check if the field type is a Literal
            if get_origin(field_.type) is Literal:
                # Extract the first literal value and determine its base type
                literal_values = get_args(field_.type)
                field_type = type(literal_values[0]) if literal_values else str
            else:
                # For non-Literal types, use the existing logic
                field_type = (
                    get_args(field_.type)[0] if get_args(field_.type) else field_.type
                )

            # Check if the field type is another BaseModel (nested struct)
            if isinstance(field_type, type) and issubclass(field_type, BaseModel):
                # Recursively fetch nested field types
                nested_types = cls._get_field_types(field_type)
                for nested_name, nested_type in nested_types.items():
                    if nested_name in field_types:
                        raise KeyError(
                            f"Key conflict detected: '{nested_name}' already exists in the field types."
                        )
                    field_types[nested_name] = nested_type
            else:
                # Add non-nested fields directly
                if field_name in field_types:
                    raise KeyError(
                        f"Key conflict detected: '{field_name}' already exists in the field types."
                    )
                field_types[field_name] = field_type

        return field_types

    @classmethod
    def null(cls) -> "BaseModel":
        """Create an instance with null values for each field."""
        null_values = {}
        field_types = cls._get_field_types(cls)

        for field_ in msgspec.structs.fields(cls):
            field_name = field_.name
            field_type = field_.type

            try:
                base_type = field_types[field_name]
            except KeyError:
                base_type = field_type

            if base_type is str:
                null_values[field_name] = ""
            elif base_type in {int, float}:
                null_values[field_name] = np.nan
            elif base_type is bool:
                null_values[field_name] = False
            elif base_type in {datetime.datetime, pd.Timestamp}:
                null_values[field_name] = pd.NaT
            elif get_origin(base_type) is dict:
                # Handle structured data types like bbox
                key_type, value_type = get_args(base_type)
                if key_type is str and value_type in {int, float}:
                    null_values[field_name] = {
                        key: np.nan for key in ["xmin", "ymin", "xmax", "ymax"]
                    }
                else:
                    null_values[field_name] = {}
            elif issubclass(
                base_type,
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
                null_values[field_name] = GeometryCollection()

            elif isinstance(base_type, type) and issubclass(base_type, BaseModel):
                # Initialize nested BaseModel with its null values
                null_values[field_name] = base_type.null()

            else:
                raise TypeError(
                    f"Unhandled field type '{base_type}' for field '{field_name}'. Add support for this type."
                )

        return cls(**null_values)

    @abstractmethod
    def example(cls) -> "BaseModel":
        """Create an example instance with predefined example values."""
        ...

    def to_dict(self, flatten: bool = True) -> dict:
        """
        Convert instance to a dictionary format.

        Args:
            flatten (bool): If True, returns a flat dictionary by merging nested BaseModel instances.
                            If False, returns a hierarchical dictionary with nested structures intact.

        Returns:
            dict: The instance represented as a dictionary.

        Raises:
            KeyError: If key conflicts occur during flattening.
        """
        if not flatten:
            return msgspec.structs.asdict(self)

        result = {}

        # Iterate over fields and their metadata
        for field_name in self.__defined_struct_fields__:
            field_value = getattr(self, field_name, None)

            # Flatten nested BaseModel instances
            if isinstance(field_value, BaseModel):
                nested_dict = field_value.to_dict(
                    flatten=True
                )  # Recursively call to_dict()

                # Check for key conflicts and raise KeyError if any conflict is found
                for nested_key, nested_value in nested_dict.items():
                    if nested_key in result:
                        raise KeyError(
                            f"Key conflict detected: '{nested_key}' already exists in the parent dictionary "
                            f"while flattening field '{field_name}'."
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

    def validate(self) -> bool:
        """
        Validate the current instance against its schema using JSON serialization.

        Returns:
            bool: True if the instance is valid, otherwise False.
        """
        try:
            # Serialize the instance to JSON and attempt to decode it back
            json_data = self.to_json()
            self.decode(json_data.encode())
            return True
        except msgspec.ValidationError as e:
            print(f"Validation Error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected Error: {e}")
            return False

    def to_meta(
        self, mode: Literal["pandas", "geoparquet"] = "pandas"
    ) -> Dict[str, str]:
        """
        Generate a dictionary mapping field names to their corresponding types.

        Parameters:
            mode (str): Either 'pandas' (default) or 'geoparquet'. Determines the type system to use.

        Returns:
            Dict[str, str]: A dictionary mapping field names to their respective types.

        Raises:
            KeyError: If duplicate keys are detected in nested fields.
            ValueError: If the `mode` is not supported.
        """
        # Select the appropriate type map
        if mode == "pandas":
            type_map = PANDAS_TYPE_MAP
        elif mode == "geoparquet":
            type_map = GEOPARQUET_TYPE_MAP
        else:
            raise ValueError(
                f"Unsupported mode '{mode}'. Use 'pandas' or 'geoparquet'."
            )

        meta = {}
        for field_name, field_type in self.__field_types__.items():
            try:
                # Map the type to the selected system
                meta[field_name] = type_map.get(field_type, type_map[object])
            except KeyError:
                raise KeyError(
                    f"Type '{field_type}' for field '{field_name}' is not supported in the {mode} type map."
                )

        return meta

    def to_frame(self) -> pd.DataFrame:
        """
        Convert instance to a DataFrame format.
        """
        data = self.to_dict()
        if "geometry" in data:
            return gpd.GeoDataFrame([data], geometry="geometry", crs="EPSG:4326")
        return pd.DataFrame([data])

    def empty_frame(self) -> pd.DataFrame:
        """
        Create an empty DataFrame with appropriate column types.
        """
        _meta = self.to_meta()

        if "geometry" in _meta:
            empty_data = {
                col: pd.Series(dtype=col_type)
                if col_type != GeometryCollection()
                else []
                for col, col_type in _meta.items()
            }
            return gpd.GeoDataFrame(empty_data, geometry="geometry", crs="EPSG:4326")

        # For non-geometric data, return a regular DataFrame
        empty_data = {col: pd.Series(dtype=col_type) for col, col_type in _meta.items()}
        return pd.DataFrame(empty_data)

    @classmethod
    def from_json(cls, json_str: str) -> "BaseModel":
        """Create an instance from JSON string."""
        decoder = msgspec.json.Decoder(cls, dec_hook=decode_custom)
        return decoder.decode(json_str.encode())

    @classmethod
    def from_dict(cls, data: Dict[str, Any], flatten: bool = True) -> "BaseModel":
        """
        Create an instance from a dictionary, handling both flat and nested structures.

        Args:
            data (Dict[str, Any]): The input data dictionary.
            flatten (bool): Whether the dictionary is in flat format.

        Returns:
            BaseModel: The instantiated object.
        """

        def split_fields(record: dict, class_: Type[msgspec.Struct]) -> dict:
            """
            Utility to split a dictionary into fields specific to a class based on its annotations.
            """
            return {
                key: value
                for key, value in record.items()
                if key in class_.__annotations__
            }

        # If the dictionary is flattened, split and reconstruct nested fields
        if flatten:
            nested_data = {}
            for field in msgspec.structs.fields(cls):
                field_name = field.name
                field_type = field.type

                # Check if the field type is a nested BaseModel
                if isinstance(field_type, type) and issubclass(field_type, BaseModel):
                    # Extract relevant fields for the nested model
                    nested_fields = split_fields(data, field_type)
                    # Create an instance of the nested model
                    nested_data[field_name] = field_type.from_dict(nested_fields)
                elif field_name in data:
                    # For non-nested fields, directly add to the data
                    nested_data[field_name] = data[field_name]
            return cls(**nested_data)

        # If the dictionary is not flattened, pass it directly to the constructor
        return cls(**data)

    @classmethod
    def from_frame(cls, frame: gpd.GeoDataFrame) -> "BaseModel":
        """Create an instance from a GeoDataFrame row."""
        data = frame.iloc[0].to_dict()
        return cls.from_dict(data)


class User(BaseModel):
    """Structured data type for user management."""

    name: str
    user_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    datetime_created: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC).isoformat()
    )
    formatted_name: str = ""  # Will be computed in `__post_init__`

    def __post_init__(self):
        """Automatically format the name upon initialization."""
        self.formatted_name = self._format_name(self.name)

    @staticmethod
    def _format_name(name: str) -> str:
        """Format user name into a normalized string."""
        name = unicodedata.normalize("NFD", name)
        name = "".join(char for char in name if unicodedata.category(char) != "Mn")
        name = name.lower()
        name = re.sub(r"\s+", "-", name)
        name = re.sub(r"[^a-z0-9\-]", "", name)
        return name


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
            "transect_id": "cl32408s01tr00223948",
            "geometry": LineString(
                [
                    [4.287529606158882, 52.106643659044614],
                    [4.266728801968574, 52.11926398930266],
                ]
            ),
            "lon": 4.277131,
            "lat": 52.112953,
            "bearing": 313.57275,
            "osm_coastline_is_closed": False,
            "osm_coastline_length": 1014897,
            "utm_epsg": 32631,
            "bbox": {
                "xmax": 4.287529606158882,
                "xmin": 4.266728801968574,
                "ymax": 52.11926398930266,
                "ymin": 52.106643659044614,
            },
            "quadkey": "020202113000",
            "continent": "EU",
            "country": "NL",
            "common_country_name": "Netherlands",
            "common_region_name": "South Holland",
        }
        return cls(**_EXAMPLE_VALUES)


class TypologyTrainSample(BaseModel):
    transect: Transect
    user: str
    uuid: str  # universal_unique_id = uuid.uuid4().hex[:12]
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
            "user": "floris-calkoen",
            "uuid": "3b984582ecd6",
            "datetime_created": datetime.datetime(2024, 1, 9, 12, 0),
            "datetime_updated": datetime.datetime(2024, 1, 11, 12, 0),
            "shore_type": "sandy_gravel_or_small_boulder_sediments",
            "coastal_type": "sediment_plain",
            "landform_type": "mainland_coast",
            "is_built_environment": "true",
            "has_defense": "true",
            "is_challenging": False,
            "comment": "This is an example transect including a comment.",
            "link": "https://example.com/link-to-google-street-view",
            "confidence": "high",
            "is_validated": True,
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
            "pred_shore_type": "sandy_gravel_or_small_boulder_sediments",
            "pred_coastal_type": "sediment_plain",
            "pred_has_defense": "true",
            "pred_is_built_environment": "false",
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
            "pred_shore_type": "sandy_gravel_or_small_boulder_sediments",
            "pred_coastal_type": "cliffed_or_steep",
            "pred_has_defense": "true",
            "pred_is_built_environment": "false",
        }
        return cls(**_EXAMPLE_VALUES)


# Union of all possible classes for decoding
ModelUnion = Union[
    User,
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
    bearing=40.0,
    bbox=bbox,
)

# NOTE: continue with TypologyTrainSample.null().to_frame()
train_sample = TypologyTrainSample(
    transect=transect,
    user="floris-calkoen",
    uuid="3b984582ecd6",
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

# nulls = Transect.null()
TypologyTrainSample.null()
train_sample.to_dict()
train_sample.to_meta()
TypologyTrainSample.example()
schema = msgspec.json.schema(Transect, schema_hook=custom_schema_hook)
import json
import pathlib

with (pathlib.Path.cwd() / "transect_schema.json").open("w") as f:
    f.write(json.dumps(schema, indent=2))

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
