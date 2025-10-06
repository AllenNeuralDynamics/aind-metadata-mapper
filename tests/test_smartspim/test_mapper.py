"""Unit tests for SmartSPIM mapper"""

import json
import unittest
from pathlib import Path
from datetime import datetime

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema_models.modalities import Modality
from aind_metadata_mapper.smartspim.mapper import SmartspimMapper


class TestSmartspimMapper(unittest.TestCase):
    """Test cases for SmartspimMapper"""

    def setUp(self):
        """Set up test fixtures"""
        self.mapper = SmartspimMapper()

        # Load test data
        test_data_path = Path(__file__).parent.parent / "resources" / "smartspim" / "smartspim.json"
        with open(test_data_path, "r") as f:
            self.test_metadata = json.load(f)

    def test_transform_returns_acquisition(self):
        """Test that transform method returns an Acquisition object"""
        result = self.mapper.transform(self.test_metadata)

        self.assertIsInstance(result, Acquisition)

    def test_transform_basic_fields(self):
        """Test that basic fields are correctly mapped"""
        result = self.mapper.transform(self.test_metadata)

        # Check basic metadata fields
        self.assertEqual(result.subject_id, "762444")
        self.assertEqual(result.specimen_id, "BRN00000292")
        self.assertEqual(result.instrument_id, "440_SmartSPIM2_20240327")
        self.assertEqual(result.acquisition_type, "SmartSPIM")

        # Check protocol information
        expected_protocol = ["https://dx.doi.org/10.17504/protocols.io.3byl4jo1rlo5/v1"]
        self.assertEqual(result.protocol_id, expected_protocol)

    def test_transform_timestamps(self):
        """Test that timestamps are correctly parsed and set"""
        result = self.mapper.transform(self.test_metadata)

        # Check that start and end times are datetime objects
        self.assertIsInstance(result.acquisition_start_time, datetime)
        self.assertIsInstance(result.acquisition_end_time, datetime)

        # Check that times match expected values from test data
        expected_start = datetime.fromisoformat("2025-07-17T01:47:42")
        expected_end = datetime.fromisoformat("2025-07-17T01:48:42")

        self.assertEqual(result.acquisition_start_time, expected_start)
        self.assertEqual(result.acquisition_end_time, expected_end)

    def test_transform_data_streams(self):
        """Test that data streams are correctly created"""
        result = self.mapper.transform(self.test_metadata)

        # Should have one data stream
        self.assertEqual(len(result.data_streams), 1)

        data_stream = result.data_streams[0]

        # Check modality
        self.assertEqual(data_stream.modalities, [Modality.SPIM])

        # Check timestamps match acquisition times
        self.assertEqual(data_stream.stream_start_time, result.acquisition_start_time)
        self.assertEqual(data_stream.stream_end_time, result.acquisition_end_time)

    def test_transform_imaging_channels(self):
        """Test that imaging channels are correctly processed"""
        result = self.mapper.transform(self.test_metadata)

        data_stream = result.data_streams[0]

        # Should have configurations
        self.assertGreater(len(data_stream.configurations), 0)

        # Find the imaging config
        imaging_config = None
        for config in data_stream.configurations:
            if hasattr(config, "channels"):
                imaging_config = config
                break

        self.assertIsNotNone(imaging_config, "Should have an imaging configuration")

        # Check that we have channels corresponding to the imaging channels in metadata
        expected_channels = [
            "Laser = 488; Emission Filter = 525/45",
            "Laser = 561; Emission Filter = 593/40",
            "Laser = 639; Emission Filter = 667/30",
        ]

        self.assertEqual(len(imaging_config.channels), len(expected_channels))

    def test_transform_with_chamber_immersion(self):
        """Test that chamber immersion medium creates sample chamber config"""
        result = self.mapper.transform(self.test_metadata)

        data_stream = result.data_streams[0]

        # Should have at least 2 configurations (imaging + chamber)
        self.assertGreaterEqual(len(data_stream.configurations), 2)

        # Should have a sample chamber config since test data has chamber_immersion_medium
        has_chamber_config = any(hasattr(config, "chamber_immersion") for config in data_stream.configurations)
        self.assertTrue(
            has_chamber_config,
            "Should have chamber configuration when immersion medium is specified",
        )

    def test_transform_experimenter_handling(self):
        """Test experimenter field handling (None in test data)"""
        result = self.mapper.transform(self.test_metadata)

        # Test data has experimenter_name as null, so should be empty list
        self.assertEqual(result.experimenters, [])

    def test_transform_with_experimenter(self):
        """Test experimenter field when experimenter name is provided"""
        # Modify test data to include experimenter name
        test_data_copy = self.test_metadata.copy()
        test_data_copy["slims_metadata"]["experimenter_name"] = "Test Experimenter"

        result = self.mapper.transform(test_data_copy)

        self.assertEqual(result.experimenters, ["Test Experimenter"])

    def test_transform_active_devices(self):
        """Test that active devices are populated"""
        result = self.mapper.transform(self.test_metadata)

        data_stream = result.data_streams[0]

        # Should have active devices
        self.assertIsNotNone(data_stream.active_devices)
        self.assertGreater(len(data_stream.active_devices), 0)

    def test_transform_wavelength_power_extraction(self):
        """Test that wavelength and power information is extracted correctly"""
        result = self.mapper.transform(self.test_metadata)

        # This is a more detailed test to ensure the mapper correctly processes
        # the wavelength config from the test data
        data_stream = result.data_streams[0]

        # Find imaging config
        imaging_config = None
        for config in data_stream.configurations:
            if hasattr(config, "channels"):
                imaging_config = config
                break

        self.assertIsNotNone(imaging_config)

        # Check that channels have light sources with proper wavelengths
        for channel in imaging_config.channels:
            if hasattr(channel, "light_sources") and channel.light_sources:
                # Each channel should have at least one light source
                self.assertGreater(len(channel.light_sources), 0)

                # Light sources should have wavelength information
                for light_source in channel.light_sources:
                    if hasattr(light_source, "wavelength"):
                        self.assertIsNotNone(light_source.wavelength)

    def test_invalid_metadata_structure(self):
        """Test handling of invalid metadata structure"""
        # Test with empty dict
        with self.assertRaises(Exception):
            self.mapper.transform({})

        # Test with missing required fields
        invalid_metadata = {"acquisition_type": "SmartSPIM"}
        with self.assertRaises(Exception):
            self.mapper.transform(invalid_metadata)

    def test_stimulus_epochs_empty(self):
        """Test that stimulus epochs is initialized as empty list"""
        result = self.mapper.transform(self.test_metadata)

        self.assertEqual(result.stimulus_epochs, [])

    def test_metadata_validation(self):
        """Test that the SmartspimModel validation works correctly"""
        # This test ensures that the metadata structure matches what SmartspimModel expects
        try:
            result = self.mapper.transform(self.test_metadata)
            # If we get here without exception, validation passed
            self.assertIsInstance(result, Acquisition)
        except Exception as e:
            self.fail(f"Metadata validation failed: {str(e)}")


if __name__ == "__main__":
    unittest.main()
