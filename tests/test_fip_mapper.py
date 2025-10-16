"""Tests for FIP mapper."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from aind_metadata_mapper.fip.mapper import FIPMapper


class TestFIPMapper(unittest.TestCase):
    """Test cases for FIPMapper."""

    def setUp(self):
        """Set up test fixtures."""
        self.mapper = FIPMapper()

        # Mock external API calls to metadata service
        self.patcher_measurements = patch.object(
            FIPMapper,
            "_parse_intended_measurements",
            return_value={
                "Fiber_0": {"R": "jRCaMP1b", "G": "dLight", "B": None, "Iso": "dLight"},
                "Fiber_1": {"R": "jRCaMP1b", "G": "dLight", "B": None, "Iso": "dLight"},
            },
        )
        self.patcher_fibers = patch.object(
            FIPMapper, "_parse_implanted_fibers", return_value=[0, 1]  # Two implanted fibers
        )
        self.patcher_measurements.start()
        self.patcher_fibers.start()
        # Load fixture of example intermediate data
        with open("tests/fixtures/fip_intermediate.json", "r", encoding="utf-8") as f:
            self.example_intermediate_data = json.load(f)

    def tearDown(self):
        """Clean up test fixtures."""
        self.patcher_measurements.stop()
        self.patcher_fibers.stop()

    def test_mapper_initialization(self):
        """Test mapper can be instantiated."""
        mapper = FIPMapper()
        self.assertIsInstance(mapper, FIPMapper)

    def test_transform_basic(self):
        """Test basic transformation from intermediate to Acquisition."""
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

        self.assertEqual(acquisition.subject_id, "12345")
        self.assertEqual(acquisition.instrument_id, "FIP_Rig_1")
        self.assertEqual(acquisition.acquisition_type, "FIP")
        self.assertEqual(len(acquisition.experimenters), 2)
        self.assertEqual(acquisition.experimenters[0], "Foo Bar")

    def test_ethics_review_id_mapping(self):
        """Test ethics review ID is correctly mapped to ethics_review_id list."""
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

        self.assertIsNotNone(acquisition.ethics_review_id)
        self.assertEqual(len(acquisition.ethics_review_id), 1)
        self.assertEqual(acquisition.ethics_review_id[0], "2115")

    def test_ethics_review_id_none(self):
        """Test ethics_review_id is None when ethics_review_id is None."""
        data = self.example_intermediate_data.copy()
        data["ethics_review_id"] = None

        acquisition = self.mapper._transform(SimpleNamespace(**data))
        self.assertIsNone(acquisition.ethics_review_id)

    def test_subject_details(self):
        """Test subject details are correctly mapped."""
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

        self.assertIsNotNone(acquisition.subject_details)
        self.assertEqual(acquisition.subject_details.mouse_platform_name, "wheel")
        self.assertAlmostEqual(float(acquisition.subject_details.animal_weight_prior), 25.3)
        self.assertAlmostEqual(float(acquisition.subject_details.animal_weight_post), 25.5)

    def test_data_stream_created(self):
        """Test data stream is created with correct modality."""
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

        self.assertEqual(len(acquisition.data_streams), 1)
        data_stream = acquisition.data_streams[0]

        self.assertEqual(len(data_stream.modalities), 1)
        from aind_data_schema_models.modalities import Modality

        self.assertEqual(data_stream.modalities[0], Modality.FIB)

    def test_active_devices(self):
        """Test active devices list is populated."""
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))
        data_stream = acquisition.data_streams[0]

        self.assertIn("FIP_Rig_1", data_stream.active_devices)
        self.assertIn("LED_UV", data_stream.active_devices)
        self.assertIn("LED_BLUE", data_stream.active_devices)
        self.assertIn("Camera_Green Iso", data_stream.active_devices)
        self.assertIn("Camera_Red", data_stream.active_devices)
        self.assertIn("cuTTLefishFip", data_stream.active_devices)

    def test_configurations_built(self):
        """Test device configurations are created."""
        acquisition = self.mapper.transform(self.example_intermediate_data)
        data_stream = acquisition.data_streams[0]

        self.assertGreater(len(data_stream.configurations), 0)

        config_types = [type(c).__name__ for c in data_stream.configurations]
        self.assertIn("LightEmittingDiodeConfig", config_types)
        self.assertIn("PatchCordConfig", config_types)

        # Verify DetectorConfig exists within Channel objects
        patch_cords = [c for c in data_stream.configurations if type(c).__name__ == "PatchCordConfig"]
        self.assertGreater(len(patch_cords), 0)
        self.assertGreater(len(patch_cords[0].channels), 0)
        self.assertIsNotNone(patch_cords[0].channels[0].detector)

    def test_timezone_handling(self):
        """Test session times are properly timezone-aware."""
        acquisition = self.mapper.transform(self.example_intermediate_data)

        self.assertIsNotNone(acquisition.acquisition_start_time.tzinfo)
        self.assertIsNotNone(acquisition.acquisition_end_time.tzinfo)

        self.assertLess(acquisition.acquisition_start_time, acquisition.acquisition_end_time)

    def test_time_swap_if_inverted(self):
        """Test that start/end times are swapped if provided in wrong order."""
        data = self.example_intermediate_data.copy()
        data["session_start_time"] = "2025-07-18T13:00:00-07:00"
        data["session_end_time"] = "2025-07-18T12:00:00-07:00"

        acquisition = self.mapper._transform(SimpleNamespace(**data))

        self.assertLess(acquisition.acquisition_start_time, acquisition.acquisition_end_time)

    def test_notes_preserved(self):
        """Test notes field is preserved."""
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))
        self.assertEqual(acquisition.notes, "Test session")

    def test_stimulus_epochs_empty(self):
        """Test stimulus_epochs is an empty list (FIP typically has no stimuli)."""
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))
        self.assertEqual(len(acquisition.stimulus_epochs), 0)


if __name__ == "__main__":
    unittest.main()
