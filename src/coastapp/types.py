import datetime
from typing import Literal, Optional
import json

from pydantic import BaseModel
from shapely.geometry import LineString, shape


# Helper functions for geometry serialization/deserialization
def serialize_geometry(geometry: LineString) -> str:
    return geometry.wkt


def deserialize_geometry(geometry_wkt: str) -> LineString:
    return shape(
        {
            "type": "LineString",
            "coordinates": [
                (float(coord) for coord in pair.split())
                for pair in geometry_wkt.split(", ")
            ],
        }
    )


# Transect Class
class Transect(BaseModel):
    transect_id: str
    geometry: LineString  # Allow Shapely geometry
    lon: Optional[float] = None
    lat: Optional[float] = None
    bearing: Optional[float] = None
    osm_coastline_is_closed: Optional[bool] = None
    osm_coastline_length: Optional[int] = None
    utm_epsg: Optional[int] = None
    bbox: Optional[dict] = None
    quadkey: Optional[str] = None
    continent: Optional[str] = None
    country: Optional[str] = None
    common_country_name: Optional[str] = None
    common_region_name: Optional[str] = None

    # Pydantic configuration
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            LineString: serialize_geometry,  # Serialize LineString to WKT
        }

    @classmethod
    def custom_parse(cls, data: dict):
        """Custom deserialization for Transect."""
        data["geometry"] = deserialize_geometry(data["geometry"])
        return cls(**data)


# TypologyTrainSample Class
class TypologyTrainSample(BaseModel):
    transect: Transect
    user: str
    uuid: str
    datetime_created: datetime.datetime
    datetime_updated: datetime.datetime
    shore_type: Literal[
        "sandy_gravel_or_small_boulder_sediments",
        "muddy_sediments",
        "rocky_shore_platform_or_large_boulders",
        "no_sediment_or_shore_platform",
        "ice_or_tundra",
    ]
    coastal_type: Literal[
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
    landform_type: Literal[
        "mainland_coast",
        "estuary",
        "barrier_island",
        "barrier",
        "pocket_beach",
        "spit",
        "enh:bay",
    ]
    is_built_environment: Literal["true", "false"]
    has_defense: Literal["true", "false"]
    is_challenging: bool
    comment: str
    link: str
    confidence: str
    is_validated: bool

    class Config:
        json_encoders = {
            datetime.datetime: lambda dt: dt.isoformat(),  # Serialize datetime to ISO format
        }

    @classmethod
    def custom_parse(cls, data: dict):
        """Custom deserialization for TypologyTrainSample."""
        data["transect"] = Transect.custom_parse(data["transect"])
        return cls(**data)


# Example Usage
if __name__ == "__main__":
    # Create a Transect instance
    transect = Transect(
        transect_id="T001",
        geometry=LineString([(34, 45), (44, 55)]),
        lon=40.0,
        lat=45.0,
        bearing=90.0,
    )

    # Create a TypologyTrainSample instance
    train_sample = TypologyTrainSample(
        transect=transect,
        user="example_user",
        uuid="123e4567-e89b-12d3-a456-426614174000",
        datetime_created=datetime.datetime.now(),
        datetime_updated=datetime.datetime.now(),
        shore_type="rocky_shore_platform_or_large_boulders",
        coastal_type="cliffed_or_steep",
        landform_type="mainland_coast",
        is_built_environment="false",
        has_defense="true",
        is_challenging=False,
        comment="Test comment",
        link="https://example.com",
        confidence="high",
        is_validated=True,
    )

    # Serialize to JSON
    train_sample_json = train_sample.model_dump_json()
    print("Serialized JSON:", train_sample_json)

    # Deserialize from JSON
    loaded_sample = TypologyTrainSample.custom_parse(json.loads(train_sample_json))
    print("Deserialized Object:", loaded_sample)
