import unittest

import geopandas as gpd
from shapely.geometry import LineString

from coastapp.specification import (
    BaseModel,
    Transect,
    TypologyInferenceSample,
    TypologyTestSample,
    TypologyTrainSample,
)


class TestBaseModelMethods(unittest.TestCase):
    def setUp(self):
        """Set up example instances for tests."""
        self.transect = Transect.example()
        self.train_sample = TypologyTrainSample.example()
        self.test_sample = TypologyTestSample.example()
        self.inference_sample = TypologyInferenceSample.example()

    def test_to_dict(self):
        """Test to_dict method."""
        transect_dict = self.transect.to_dict()
        self.assertIsInstance(transect_dict, dict)
        self.assertIn("transect_id", transect_dict)
        self.assertEqual(transect_dict["transect_id"], "cl32408s01tr00223948")

        train_sample_dict = self.train_sample.to_dict()
        self.assertIsInstance(train_sample_dict, dict)
        self.assertIn("transect_id", train_sample_dict)
        self.assertEqual(train_sample_dict["transect_id"], "cl32408s01tr00223948")

    def test_to_meta(self):
        """Test to_meta method."""
        transect_meta = self.transect.to_meta()
        self.assertIsInstance(transect_meta, dict)
        self.assertIn("transect_id", transect_meta)
        self.assertEqual(transect_meta["transect_id"], "object")

        train_sample_meta = self.train_sample.to_meta()
        self.assertIn("shore_type", train_sample_meta)
        self.assertEqual(train_sample_meta["shore_type"], "object")

    def test_to_frame(self):
        """Test to_frame method."""
        transect_frame = self.transect.to_frame()
        self.assertIsInstance(transect_frame, gpd.GeoDataFrame)
        self.assertIn("transect_id", transect_frame.columns)
        self.assertIn("geometry", transect_frame.columns)

        train_sample_frame = self.train_sample.to_frame()
        self.assertIsInstance(train_sample_frame, gpd.GeoDataFrame)
        self.assertIn("shore_type", train_sample_frame.columns)

    def test_json_round_trip(self):
        """Test JSON round-trip functionality."""
        train_sample_json = self.train_sample.to_json()
        self.assertIsInstance(train_sample_json, str)

        decoded_sample = BaseModel().decode(train_sample_json.encode())
        self.assertEqual(self.train_sample, decoded_sample)

    def test_decode(self):
        """Test decoding from JSON."""
        json_data = self.train_sample.to_json()
        decoded_instance = BaseModel().decode(json_data.encode())
        self.assertIsInstance(decoded_instance, TypologyTrainSample)
        self.assertEqual(decoded_instance.transect.transect_id, "cl32408s01tr00223948")

    def test_geometry_serialization(self):
        """Test serialization and deserialization of geometries."""
        geometry = self.transect.geometry
        self.assertIsInstance(geometry, LineString)

        encoded_json = self.transect.to_json()
        decoded_instance = BaseModel().decode(encoded_json.encode())
        self.assertEqual(decoded_instance.geometry, self.transect.geometry)

    def test_literal_fields(self):
        """Test handling of Literal fields."""
        train_sample_meta = self.train_sample.to_meta()
        self.assertIn("shore_type", train_sample_meta)
        self.assertEqual(train_sample_meta["shore_type"], "object")


if __name__ == "__main__":
    unittest.main()
