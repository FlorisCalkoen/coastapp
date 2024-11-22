from typing import Dict

from sklearn.preprocessing import OneHotEncoder


def get_one_hot_encoders(
    label_encoding: Dict[str, Dict[str, int]],
) -> Dict[str, OneHotEncoder]:
    """Create a dictionary of OneHotEncoders from a label encoding specification."""
    encoders = {}
    for field, classes in label_encoding.items():
        encoders[field] = OneHotEncoder(
            categories=[tuple(classes)], sparse_output=False
        ).fit([[cls] for cls in classes])
    return encoders


SHORE_TYPE_ENCODING = {
    "sandy_gravel_or_small_boulder_sediments": 0,
    "muddy_sediments": 1,
    "rocky_shore_platform_or_large_boulders": 2,
    "no_sediment_or_shore_platform": 3,
    "ice_or_tundra": 4,
}


COASTAL_TYPE_ENCODING = {
    "cliffed_or_steep": 0,
    "moderately_sloped": 1,
    "bedrock_plain": 2,
    "sediment_plain": 3,
    "dune": 4,
    "wetland": 5,
    "coral": 6,
    "inlet": 7,
    "engineered_structures": 8,
}

IS_BUILT_ENVIRONMENT_ENCODING = {
    "true": 1,
    "false": 0,
}

HAS_DEFENSE_ENCODING = {
    "true": 1,
    "false": 0,
}

LANDFORM_TYPE_ENCODING = {
    "mainland_coast": 0,
    "estuary": 1,
    "barrier_island": 2,
    "barrier": 3,
    "pocket_beach": 4,
    "spit": 5,
    "enh:bay": 6,
}

LABEL_ENCODING = {
    "shore_type": SHORE_TYPE_ENCODING,
    "coastal_type": COASTAL_TYPE_ENCODING,
    "is_built_environment": IS_BUILT_ENVIRONMENT_ENCODING,
    "has_defense": HAS_DEFENSE_ENCODING,
    "landform_type": LANDFORM_TYPE_ENCODING,
}

ONE_HOT_ENCODERS = get_one_hot_encoders(LABEL_ENCODING)

from typing import Literal

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
