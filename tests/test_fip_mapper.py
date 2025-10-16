"""Tests for FIP mapper."""

import unittest
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
            'get_intended_measurements',
            return_value={
                'Fiber_0': {'R': 'jRCaMP1b', 'G': 'dLight', 'B': None, 'Iso': 'dLight'},
                'Fiber_1': {'R': 'jRCaMP1b', 'G': 'dLight', 'B': None, 'Iso': 'dLight'},
            }
        )
        self.patcher_fibers = patch.object(
            FIPMapper,
            'get_implanted_fibers',
            return_value=[0, 1]  # Two implanted fibers
        )
        self.patcher_measurements.start()
        self.patcher_fibers.start()
        
        self.example_intermediate_data = {
            "job_settings_name": "FIP",
            "experimenter_full_name": ["Foo Bar", "Test User"],
            "session_start_time": "2025-07-18T12:32:35.275046-07:00",
            "session_end_time": "2025-07-18T12:49:22.448358-07:00",
            "subject_id": "12345",
            "rig_id": "FIP_Rig_1",
            "mouse_platform_name": "wheel",
            "active_mouse_platform": False,
            "data_streams": [],
            "session_type": "FIP",
            "ethics_review_id": "2115",
            "notes": "Test session",
            "anaesthesia": None,
            "animal_weight_post": 25.5,
            "animal_weight_prior": 25.3,
            "protocol_id": [],
            "data_directory": "/data/test",
            "data_files": [],
            "rig_config": {
                "rig_name": "FIP_Rig_1",
                "camera_green_iso": {
                    "device_type": "FipCamera",
                    "name": "FipCamera",
                    "serial_number": "24521418",
                    "gain": 0.0,
                },
                "camera_red": {
                    "device_type": "FipCamera",
                    "name": "FipCamera",
                    "serial_number": "24521421",
                    "gain": 1.5,
                },
                "light_source_uv": {
                    "device_type": "LightSource",
                    "name": "LightSource",
                    "power": 1.0,
                },
                "light_source_blue": {
                    "device_type": "LightSource",
                    "name": "LightSource",
                    "power": 0.8,
                },
                "cuttlefish_fip": {
                    "device_type": "cuTTLefishFip",
                    "name": "cuTTLefishFip",
                    "who_am_i": 1407,
                },
                "roi_settings": {
                    "camera_green_iso_roi": [
                        {"center": {"x": 50.0, "y": 50.0}, "radius": 20.0},
                        {"center": {"x": 150.0, "y": 150.0}, "radius": 20.0}
                    ],
                    "camera_red_roi": [
                        {"center": {"x": 50.0, "y": 50.0}, "radius": 20.0}
                    ]
                },
            },
            "session_config": {
                "experiment": "AindPhysioFip",
                "experimenter": ["Foo Bar", "Test User"],
                "subject": "12345",
            },
            "output_directory": "/output/test"
        }
    
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
        acquisition = self.mapper.transform(self.example_intermediate_data)
        
        self.assertEqual(acquisition.subject_id, "12345")
        self.assertEqual(acquisition.instrument_id, "FIP_Rig_1")
        self.assertEqual(acquisition.acquisition_type, "FIP")
        self.assertEqual(len(acquisition.experimenters), 2)
        self.assertEqual(acquisition.experimenters[0], "Foo Bar")

    def test_ethics_review_id_mapping(self):
        """Test ethics review ID is correctly mapped to ethics_review_id list."""
        acquisition = self.mapper.transform(self.example_intermediate_data)
        
        self.assertIsNotNone(acquisition.ethics_review_id)
        self.assertEqual(len(acquisition.ethics_review_id), 1)
        self.assertEqual(acquisition.ethics_review_id[0], "2115")

    def test_ethics_review_id_none(self):
        """Test ethics_review_id is None when ethics_review_id is None."""
        data = self.example_intermediate_data.copy()
        data["ethics_review_id"] = None
        
        acquisition = self.mapper.transform(data)
        self.assertIsNone(acquisition.ethics_review_id)

    def test_subject_details(self):
        """Test subject details are correctly mapped."""
        acquisition = self.mapper.transform(self.example_intermediate_data)
        
        self.assertIsNotNone(acquisition.subject_details)
        self.assertEqual(acquisition.subject_details.mouse_platform_name, "wheel")
        self.assertAlmostEqual(float(acquisition.subject_details.animal_weight_prior), 25.3)
        self.assertAlmostEqual(float(acquisition.subject_details.animal_weight_post), 25.5)

    def test_data_stream_created(self):
        """Test data stream is created with correct modality."""
        acquisition = self.mapper.transform(self.example_intermediate_data)
        
        self.assertEqual(len(acquisition.data_streams), 1)
        data_stream = acquisition.data_streams[0]
        
        self.assertEqual(len(data_stream.modalities), 1)
        from aind_data_schema_models.modalities import Modality
        self.assertEqual(data_stream.modalities[0], Modality.FIB)

    def test_active_devices(self):
        """Test active devices list is populated."""
        acquisition = self.mapper.transform(self.example_intermediate_data)
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
        
        self.assertLess(
            acquisition.acquisition_start_time,
            acquisition.acquisition_end_time
        )

    def test_time_swap_if_inverted(self):
        """Test that start/end times are swapped if provided in wrong order."""
        data = self.example_intermediate_data.copy()
        data["session_start_time"] = "2025-07-18T13:00:00-07:00"
        data["session_end_time"] = "2025-07-18T12:00:00-07:00"
        
        acquisition = self.mapper.transform(data)
        
        self.assertLess(
            acquisition.acquisition_start_time,
            acquisition.acquisition_end_time
        )

    def test_notes_preserved(self):
        """Test notes field is preserved."""
        acquisition = self.mapper.transform(self.example_intermediate_data)
        self.assertEqual(acquisition.notes, "Test session")

    def test_stimulus_epochs_empty(self):
        """Test stimulus_epochs is an empty list (FIP typically has no stimuli)."""
        acquisition = self.mapper.transform(self.example_intermediate_data)
        self.assertEqual(len(acquisition.stimulus_epochs), 0)


if __name__ == "__main__":
    unittest.main()

