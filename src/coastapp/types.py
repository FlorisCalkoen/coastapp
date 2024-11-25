import datetime
from typing import List, Literal, Optional

import msgspec
from shapely.geometry import LineString, mapping


class Transect(msgspec.Struct):
    """
    Core schema for Transects (no prefix).
    """

    id: str
    geometry: LineString
    bbox: Optional[List[float]] = None  # [xmin, ymin, xmax, ymax]
    area: Optional[float] = None
    perimeter: Optional[float] = None
    determination_datetime: Optional[datetime.datetime] = None
    determination_method: Optional[
        Literal[
            "manual",
            "driven",
            "surveyed",
            "administrative",
            "auto-operation",
            "auto-imagery",
            "unknown",
        ]
    ] = None

    def to_dict(self) -> dict:
        """Serialize Transect to a dictionary."""
        result = msgspec.structs.asdict(self)
        if isinstance(self.geometry, LineString):
            result["geometry"] = mapping(self.geometry)  # Convert geometry to GeoJSON
        return {k: v for k, v in result.items() if v is not None}


class ExtensionBase(
    msgspec.Struct,
    kw_only=True,  # Set all fields as keyword-only to avoid ordering issues
    tag=True,
    tag_field="type",
    dict=True,
    omit_defaults=True,
    repr_omit_defaults=True,
):
    """
    Base class for extensions with a required prefix.
    """

    prefix: str

    def to_dict(self) -> dict:
        """Serialize with prefixed keys."""
        data = msgspec.structs.asdict(self)
        return {
            f"{self.prefix}{key}": value
            for key, value in data.items()
            if value is not None
        }


class TypologyExtension(ExtensionBase):
    """
    Typology extension with a 'typology:' prefix.
    """

    prefix: str
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
    confidence: Optional[str] = None
    is_built_environment: Optional[Literal["true", "false"]] = None
    has_defense: Optional[Literal["true", "false"]] = None


class ExtendedTransect(msgspec.Struct):
    """
    Combines Transect (core) with extensions.
    """

    transect: Transect
    extensions: List[ExtensionBase] = []

    def to_dict(self) -> dict:
        """Combine core and extensions into a single flat dictionary."""
        base = self.transect.to_dict()
        for extension in self.extensions:
            extension_data = extension.to_dict()
            for key in extension_data:
                if key in base:
                    raise KeyError(
                        f"Key conflict detected: '{key}' exists in both core and extension."
                    )
                base[key] = extension_data[key]
        return base


from shapely.geometry import LineString

# Core Transect
transect = Transect(
    id="T001",
    geometry=LineString([(34, 45), (44, 55)]),
    bbox=[34.0, 45.0, 44.0, 55.0],
    area=100.5,
    perimeter=40.2,
    determination_datetime=datetime.datetime.now(),
    determination_method="manual",
)

# Typology Extension
typology_extension = TypologyExtension(
    prefix="typology:",
    shore_type="sandy_gravel_or_small_boulder_sediments",
    coastal_type="cliffed_or_steep",
    landform_type="mainland_coast",
    confidence="high",
    is_built_environment="false",
    has_defense="true",
)

# Combine Core and Extension
extended_transect = ExtendedTransect(transect=transect, extensions=[typology_extension])

# Serialize to dictionary
flat_dict = extended_transect.to_dict()
print(flat_dict)
