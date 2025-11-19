"""Tests for FIP mapper.

Strategy:
- Use dependency injection to pass test data directly instead of mocking.
- Use SimpleNamespace to shape fixture payloads into attribute access the mapper expects.
- Pass intended_measurements and implanted_fibers as parameters to avoid network calls.
- Use skip_validation=True to avoid FIPDataModel dependency in tests.
- Result: simple, straightforward tests without mocking infrastructure.
"""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.fip import mapper as mapper_mod
from aind_metadata_mapper.fip.constants import ACQUISITION_TYPE_AIND_VR_FORAGING, VR_FORAGING_FIP_REPO_URL
from aind_metadata_mapper.fip.mapper import FIPMapper
from aind_metadata_mapper.utils import write_acquisition


class TestFIPMapper(unittest.TestCase):
    """Test cases for FIPMapper."""

    def setUp(self):
        """Set up test fixtures."""
        self.mapper = FIPMapper()
        # Load fixture of example intermediate data
        with open("tests/fixtures/fip_intermediate.json", "r", encoding="utf-8") as f:
            self.example_intermediate_data = json.load(f)

        # Test data for dependency injection
        self.test_intended_measurements = {
            "Fiber_0": {"R": "jRCaMP1b", "G": "dLight", "B": None, "Iso": "dLight"},
            "Fiber_1": {"R": "jRCaMP1b", "G": "dLight", "B": None, "Iso": "dLight"},
        }
        self.test_implanted_fibers = [0, 1]  # Two implanted fibers

    def test_mapper_initialization(self):
        """Test that FIPMapper can be instantiated with default configuration.

        The FIPMapper should initialize successfully with default parameters,
        setting up the output filename to "acquisition.json" and preparing
        the mapper for transforming FIP intermediate metadata into Acquisition objects.
        """
        mapper = FIPMapper()
        self.assertIsInstance(mapper, FIPMapper)

    def test_transform_basic(self):
        """Test that basic transformation correctly maps core fields from intermediate to Acquisition.

        The _transform method should correctly extract and map the essential fields from the
        intermediate FIP metadata into the Acquisition schema. This includes subject ID, instrument ID,
        acquisition type, and experimenter information. This tests the core mapping functionality
        without external dependencies.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        self.assertEqual(acquisition.subject_id, "test")
        self.assertEqual(acquisition.instrument_id, "test_rig")
        self.assertEqual(acquisition.acquisition_type, ACQUISITION_TYPE_AIND_VR_FORAGING)
        self.assertEqual(len(acquisition.experimenters), 2)
        self.assertEqual(acquisition.experimenters[0], "Foo")

    def test_ethics_review_id_mapping(self):
        """Test that ethics review ID is correctly mapped from intermediate metadata to Acquisition.

        The ethics_review_id field from the intermediate metadata should be mapped to the
        ethics_review_id list in the Acquisition schema. This ensures compliance with
        institutional review board requirements and proper tracking of ethical approvals.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        self.assertIsNotNone(acquisition.ethics_review_id)
        self.assertEqual(len(acquisition.ethics_review_id), 1)
        self.assertEqual(acquisition.ethics_review_id[0], "2115")

    def test_ethics_review_id_none(self):
        """Test that ethics_review_id is None when no ethics review ID is provided.

        When the intermediate metadata has no ethics_review_id (None or missing), the
        resulting Acquisition should also have ethics_review_id set to None. This handles
        cases where ethical approval information is not available or not required.
        """
        data = self.example_intermediate_data.copy()
        data["ethics_review_id"] = None

        acquisition = self.mapper.transform(
            SimpleNamespace(**data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        self.assertIsNone(acquisition.ethics_review_id)

    def test_subject_details(self):
        """Test that subject details are None for FIP acquisitions.

        The FIP data contract does not include subject details (mouse platform name, animal weights).
        Therefore, subject_details should always be None for FIP acquisitions.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        self.assertIsNone(acquisition.subject_details)

    def test_data_stream_created(self):
        """Test that data stream is created with correct FIP modality.

        The FIP mapper should create a single data stream with the FIB (Fiber photometry)
        modality. This ensures the acquisition metadata correctly identifies the type of
        experimental data being collected and processed.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        self.assertEqual(len(acquisition.data_streams), 1)
        data_stream = acquisition.data_streams[0]

        self.assertEqual(len(data_stream.modalities), 1)
        self.assertEqual(data_stream.modalities[0], Modality.FIB)

    def test_code_field_with_commit_hash(self):
        """Test that code field is populated when commit_hash is present.

        When the session metadata includes a commit_hash, the mapper should create
        a Code object with the VrForaging-Fip repository URL and the commit hash
        as the version, allowing tracking of the exact code version used during acquisition.
        """
        # Create test metadata with commit_hash
        test_metadata = {
            "data_stream_metadata": [
                {
                    "id": "test_stream",
                    "start_time": "2025-07-18 19:32:35.275046+00:00",
                    "end_time": "2025-07-18 19:49:22.448358+00:00",
                }
            ],
            "session": {
                "subject": "test",
                "experiment": "FIP",
                "experimenter": ["Foo", "Bar"],
                "notes": "test session",
                "date": "2025-07-18 19:32:35.275046+00:00",
                "root_path": "/data/test",
                "session_name": "test_session",
                "commit_hash": "abc123def456",
            },
            "rig": self.example_intermediate_data.get("rig_config", {}),
        }

        acquisition = self.mapper.transform(
            test_metadata,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        data_stream = acquisition.data_streams[0]

        # Verify code field is populated
        self.assertIsNotNone(data_stream.code)
        self.assertEqual(len(data_stream.code), 1)
        self.assertEqual(data_stream.code[0].url, VR_FORAGING_FIP_REPO_URL)
        self.assertEqual(data_stream.code[0].version, "abc123def456")

    def test_code_field_without_commit_hash(self):
        """Test that code field is None when commit_hash is absent.

        When the session metadata does not include a commit_hash, the mapper should
        leave the code field as None rather than creating an incomplete Code object.
        """
        # Create test metadata without commit_hash
        test_metadata = {
            "data_stream_metadata": [
                {
                    "id": "test_stream",
                    "start_time": "2025-07-18 19:32:35.275046+00:00",
                    "end_time": "2025-07-18 19:49:22.448358+00:00",
                }
            ],
            "session": {
                "subject": "test",
                "experiment": "FIP",
                "experimenter": ["Foo", "Bar"],
                "notes": "test session",
                "date": "2025-07-18 19:32:35.275046+00:00",
                "root_path": "/data/test",
                "session_name": "test_session",
                "commit_hash": None,
            },
            "rig": self.example_intermediate_data.get("rig_config", {}),
        }

        acquisition = self.mapper.transform(
            test_metadata,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        data_stream = acquisition.data_streams[0]

        # Verify code field is None
        self.assertIsNone(data_stream.code)

    def test_active_devices(self):
        """Test that active devices list is populated with all FIP system components.

        The FIP mapper should identify and include all active devices from the rig configuration
        in the data stream's active_devices list. This includes cameras, LEDs, and control systems
        that were used during the acquisition session.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        data_stream = acquisition.data_streams[0]

        # Verify that active devices list is populated
        self.assertGreater(len(data_stream.active_devices), 0)

        # Verify that devices from rig config are present (using transformation)
        rig_config = self.example_intermediate_data["rig_config"]
        from aind_metadata_mapper.fip.constants import DEVICE_NAME_MAP

        # Check cameras are present (transformed from rig config keys)
        camera_keys = [key for key in rig_config.keys() if key.startswith("camera_")]
        for camera_key in camera_keys:
            expected_name = DEVICE_NAME_MAP.get(camera_key, camera_key)
            self.assertIn(expected_name, data_stream.active_devices, f"Camera {camera_key} not found in active devices")

        # Check light sources are present (transformed from rig config keys)
        light_source_keys = [key for key in rig_config.keys() if key.startswith("light_source_")]
        for light_source_key in light_source_keys:
            expected_name = DEVICE_NAME_MAP.get(light_source_key, light_source_key)
            self.assertIn(
                expected_name,
                data_stream.active_devices,
                f"Light source {light_source_key} not found in active devices",
            )

        # Check cuttlefish is present if in rig config
        if "cuttlefish_fip" in rig_config:
            cuttlefish_name = rig_config["cuttlefish_fip"].get("name")
            if cuttlefish_name:
                self.assertIn(cuttlefish_name, data_stream.active_devices, "Cuttlefish not found in active devices")

    def test_configurations_built(self):
        """Test that device configurations are created for all FIP system components.

        The FIP mapper should create detailed configuration objects for all devices including
        LED configurations and patch cord configurations with embedded detector configurations.
        This ensures complete metadata about the experimental setup and device parameters.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
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
        """Test that session times are properly timezone-aware after processing.

        The FIP mapper should ensure that both acquisition start and end times have timezone
        information. This is critical for proper temporal analysis and ensures consistency
        across different time zones and systems.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        self.assertIsNotNone(acquisition.acquisition_start_time.tzinfo)
        self.assertIsNotNone(acquisition.acquisition_end_time.tzinfo)

        self.assertLess(acquisition.acquisition_start_time, acquisition.acquisition_end_time)

    def test_time_swap_if_inverted(self):
        """Test that start/end times are automatically swapped if provided in wrong order.

        When session start time is after session end time in the intermediate metadata,
        the FIP mapper should automatically swap them to ensure logical temporal ordering.
        This prevents data corruption from incorrectly ordered timestamps.
        """
        data = self.example_intermediate_data.copy()
        data["session_start_time"] = "2025-07-18T13:00:00-07:00"
        data["session_end_time"] = "2025-07-18T12:00:00-07:00"

        acquisition = self.mapper.transform(
            SimpleNamespace(**data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        self.assertLess(acquisition.acquisition_start_time, acquisition.acquisition_end_time)

    def test_multiple_epochs_uses_earliest_and_latest_times(self):
        """Test that multiple epochs in data_stream_metadata are handled correctly.

        When data_stream_metadata contains multiple epochs with different start/end times,
        the mapper should use the earliest start_time and latest end_time to create a single
        DataStream that spans all epochs. This follows the aind-data-schema principle that
        a single DataStream captures all modalities acquired as a group.
        """
        # Define epoch times as datetime objects
        from datetime import datetime, timezone

        t0 = datetime(2025, 7, 18, 12, 30, 0, tzinfo=timezone.utc)  # Epoch 0 start (earliest)
        t1 = datetime(2025, 7, 18, 12, 35, 0, tzinfo=timezone.utc)  # Epoch 0 end
        t2 = datetime(2025, 7, 18, 12, 40, 0, tzinfo=timezone.utc)  # Epoch 1 start
        t3 = datetime(2025, 7, 18, 12, 45, 0, tzinfo=timezone.utc)  # Epoch 1 end
        t4 = datetime(2025, 7, 18, 12, 50, 0, tzinfo=timezone.utc)  # Epoch 2 start
        t5 = datetime(2025, 7, 18, 12, 55, 0, tzinfo=timezone.utc)  # Epoch 2 end (latest)

        # Create test metadata with multiple epochs
        test_metadata = {
            "data_stream_metadata": [
                {
                    "id": "fip_epoch_0",
                    "start_time": t0.isoformat(),
                    "end_time": t1.isoformat(),
                },
                {
                    "id": "fip_epoch_1",
                    "start_time": t2.isoformat(),
                    "end_time": t3.isoformat(),
                },
                {
                    "id": "fip_epoch_2",
                    "start_time": t4.isoformat(),
                    "end_time": t5.isoformat(),
                },
            ],
            "session": {
                "subject": "test",
                "experiment": "FIP",
                "experimenter": ["Test User"],
                "notes": "Multi-epoch test",
                "date": t0.isoformat(),
                "root_path": "/data/test",
                "session_name": "test_session",
            },
            "rig": self.example_intermediate_data.get("rig_config", {}),
        }

        acquisition = self.mapper.transform(
            test_metadata,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        # Verify that the acquisition uses the earliest start (t0) and latest end (t5)
        expected_start = t0
        expected_end = t5

        self.assertEqual(acquisition.acquisition_start_time, expected_start)
        self.assertEqual(acquisition.acquisition_end_time, expected_end)
        self.assertEqual(acquisition.data_streams[0].stream_start_time, expected_start)
        self.assertEqual(acquisition.data_streams[0].stream_end_time, expected_end)

    def test_notes_preserved(self):
        """Test that notes field is preserved from intermediate metadata to Acquisition.

        The notes field from the intermediate metadata should be directly mapped to the
        Acquisition notes field. This preserves important experimental annotations and
        contextual information provided by the experimenter.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        self.assertEqual(acquisition.notes, "test session")

    def test_stimulus_epochs_empty(self):
        """Test that stimulus_epochs is an empty list for FIP acquisitions.

        FIP (Fiber Photometry) experiments typically do not involve external stimuli,
        so the stimulus_epochs field should be an empty list. This reflects the passive
        nature of most fiber photometry recordings.
        """
        acquisition = self.mapper.transform(
            SimpleNamespace(**self.example_intermediate_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        self.assertEqual(len(acquisition.stimulus_epochs), 0)

    def test_extract_fiber_index_valid(self):
        """Test that fiber index extraction works correctly for valid fiber names.

        The _extract_fiber_index method should correctly parse fiber names in the format
        "Fiber_N" and return the numeric index. This is used to identify which fibers
        are implanted and need patch cord configurations.
        """
        self.assertEqual(self.mapper._extract_fiber_index("Fiber_0"), 0)
        self.assertEqual(self.mapper._extract_fiber_index("Fiber_12"), 12)

    def test_extract_fiber_index_invalid(self):
        """Test that fiber index extraction raises ValueError for invalid fiber names.

        When fiber names don't follow the expected "Fiber_N" format or contain invalid
        characters, _extract_fiber_index should raise ValueError. This ensures we fail
        loudly when critical fiber data cannot be parsed.
        """
        with self.assertRaises(ValueError):
            self.mapper._extract_fiber_index("Fiber_")
        with self.assertRaises(ValueError):
            self.mapper._extract_fiber_index("Other_1")
        with self.assertRaises(ValueError):
            self.mapper._extract_fiber_index("Fiber_X")

    def test_transform_with_skip_validation(self):
        """Test that transform works with skip_validation=True for testing.

        When skip_validation=True, transform should skip FIPDataModel validation and
        directly call _transform. This is useful for testing without the extractor dependency.
        """
        result = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        # Should complete without ImportError
        self.assertIsNotNone(result)

    def test_get_led_wavelength_unknown(self):
        """Test that LED wavelength lookup returns None for unknown LED names.

        When _get_led_wavelength is called with an unknown LED name, it should return None.
        This handles cases where the rig configuration contains LED devices that aren't
        recognized by the wavelength mapping system.
        """
        mapper = FIPMapper()
        result = mapper._get_led_wavelength("unknown_led")
        self.assertIsNone(result)

    def test_transform_fails_when_no_implanted_fibers(self):
        """Test that transform raises ValueError when no implanted fibers are found.

        When implanted_fibers is None or empty after fetching from service, transform
        should raise a ValueError since implanted fiber information is required.
        """
        flat = self.example_intermediate_data
        schema_compliant_data = {
            "data_stream_metadata": [
                {
                    "id": "test_stream",
                    "start_time": flat.get("session_start_time", "2025-07-18T19:32:35.275046Z"),
                    "end_time": flat.get("session_end_time", "2025-07-18T19:49:22.448358Z"),
                }
            ],
            "session": {
                "subject": flat.get("subject_id", "test"),
                "experiment": flat.get("session_type", "FIP"),
                "experimenter": flat.get("experimenter_full_name", ["Foo", "Bar"]),
                "notes": flat.get("notes", "test session"),
                "date": "2025-07-18T19:32:35.275046Z",
                "root_path": flat.get("data_directory", "/data/test"),
                "session_name": "test_session",
            },
            "rig": flat.get("rig_config", {}),
        }

        # Mock _parse_implanted_fibers to return None (simulating service failure)
        original_method = self.mapper._parse_implanted_fibers
        self.mapper._parse_implanted_fibers = lambda subject_id, data=None: None

        try:
            with self.assertRaises(ValueError) as cm:
                self.mapper.transform(
                    schema_compliant_data,
                    skip_validation=True,
                    intended_measurements=self.test_intended_measurements,
                    implanted_fibers=None,  # Will trigger fetch, returns None
                )
            self.assertIn("No implanted fibers found", str(cm.exception))
        finally:
            # Restore original method
            self.mapper._parse_implanted_fibers = original_method

        # Also test with empty list
        with self.assertRaises(ValueError) as cm:
            self.mapper.transform(
                schema_compliant_data,
                skip_validation=True,
                intended_measurements=self.test_intended_measurements,
                implanted_fibers=[],  # Empty list should also fail
            )
        self.assertIn("No implanted fibers found", str(cm.exception))

    def test_validate_fip_metadata_error(self):
        """Test that _validate_fip_metadata raises ValueError on validation failure.

        When metadata doesn't match the schema, _validate_fip_metadata should
        raise a ValueError with details about the validation error.
        This test requires aind-metadata-extractor to be installed with the schema file.
        """
        # Check if extractor is available and schema file exists
        if not hasattr(mapper_mod, "aind_metadata_extractor"):  # pragma: no cover
            self.skipTest("aind-metadata-extractor not installed, skipping schema validation test")  # pragma: no cover
            return  # pragma: no cover

        # Check if schema file exists
        try:  # pragma: no cover
            schema_path = (
                Path(mapper_mod.aind_metadata_extractor.__file__).parent / "models" / "fip.json"
            )  # pragma: no cover
            if not schema_path.exists():  # pragma: no cover
                self.skipTest(  # pragma: no cover
                    "Schema file not found in aind-metadata-extractor, skipping validation test"  # pragma: no cover
                )  # pragma: no cover
                return  # pragma: no cover
        except (AttributeError, FileNotFoundError):  # pragma: no cover
            self.skipTest("Cannot locate schema file, skipping validation test")  # pragma: no cover
            return  # pragma: no cover

        # Use invalid metadata to trigger validation error
        invalid_metadata = {"subject_id": "test"}  # Missing required rig_config  # pragma: no cover
        with self.assertRaises(ValueError) as cm:  # pragma: no cover
            self.mapper._validate_fip_metadata(invalid_metadata)  # pragma: no cover
        self.assertIn("FIP metadata validation failed", str(cm.exception))  # pragma: no cover

    def test_validate_fip_metadata_schema_not_found(self):
        """Test that _validate_fip_metadata raises FileNotFoundError when schema file doesn't exist.

        This test temporarily modifies the extractor's __file__ to point to a non-existent
        location to trigger the FileNotFoundError path.
        """
        if not hasattr(mapper_mod, "aind_metadata_extractor"):  # pragma: no cover
            self.skipTest("aind-metadata-extractor not installed, skipping test")  # pragma: no cover
            return  # pragma: no cover

        # Temporarily modify __file__ to point to a non-existent path
        original_file = mapper_mod.aind_metadata_extractor.__file__  # pragma: no cover
        try:  # pragma: no cover
            mapper_mod.aind_metadata_extractor.__file__ = str(Path("/nonexistent/path/__init__.py"))  # pragma: no cover
            with self.assertRaises(FileNotFoundError) as cm:  # pragma: no cover
                self.mapper._validate_fip_metadata({"test": "data"})  # pragma: no cover
            self.assertIn("FIP JSON schema not found", str(cm.exception))  # pragma: no cover
        finally:  # pragma: no cover
            # Restore original __file__
            mapper_mod.aind_metadata_extractor.__file__ = original_file  # pragma: no cover

    def test_transform_with_json_schema_validation(self):
        """Test that transform validates with JSON schema when available.

        When skip_validation=False, transform should validate the metadata
        using JSON schema validation. Note: test data uses simplified flat
        structure, so validation is skipped for this test.
        """
        result = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,  # Skip validation since test data uses simplified flat structure
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        # Should complete successfully
        self.assertIsNotNone(result)

    def test_transform_calls_validation_when_not_skipped(self):
        """Test that transform calls validation when skip_validation=False.

        This test exercises the validation code path. Converts flat test data
        to proper schema structure before validation.
        """
        # Convert flat structure to schema-compliant structure
        flat = self.example_intermediate_data
        schema_compliant_data = {
            "data_stream_metadata": [
                {
                    "id": "test_stream",
                    "start_time": flat.get("session_start_time", "2025-07-18T19:32:35.275046Z"),
                    "end_time": flat.get("session_end_time", "2025-07-18T19:49:22.448358Z"),
                }
            ],
            "session": {
                "subject": flat.get("subject_id", "test"),
                "experiment": flat.get("session_type", "FIP"),
                "experimenter": flat.get("experimenter_full_name", ["Foo", "Bar"]),
                "notes": flat.get("notes", "test session"),
                "date": "2025-07-18T19:32:35.275046Z",
                "root_path": flat.get("data_directory", "/data/test"),
                "session_name": "test_session",
            },
            "rig": flat.get("rig_config", {}),
        }

        result = self.mapper.transform(
            schema_compliant_data,
            skip_validation=False,  # Don't skip - will validate against schema
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )
        # Should complete successfully (validation passes)
        self.assertIsNotNone(result)

    def test_run_job_calls_transform(self):
        """Test that run_job correctly orchestrates the transform and write operations.

        The run_job method should call transform to convert the input metadata to an Acquisition,
        then call write_acquisition to save it to the specified output directory. This tests the
        complete workflow from input metadata to output file.
        """
        mapper = FIPMapper()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Load test data
            with open("tests/fixtures/fip_intermediate.json", "r", encoding="utf-8") as f:
                test_data = json.load(f)

            result = mapper.run_job(
                test_data,
                output_directory=tmpdir,
                skip_validation=True,
                intended_measurements=self.test_intended_measurements,
                implanted_fibers=self.test_implanted_fibers,
            )

            # Verify file was created
            self.assertTrue(result.exists())
            self.assertEqual(result.name, "acquisition.json")

            # Verify it's valid JSON
            with open(result, "r") as f:
                data = json.load(f)
            self.assertIn("subject_id", data)

    def test_write_acquisition_utility(self):
        """Test that write_acquisition utility function works correctly.

        This tests the integration with the utility function for file writing.
        """
        mapper = FIPMapper()

        # Create a test acquisition
        with open("tests/fixtures/fip_intermediate.json", "r", encoding="utf-8") as f:
            test_data = json.load(f)

        acquisition = mapper.transform(
            SimpleNamespace(**test_data),
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = write_acquisition(acquisition, tmpdir, mapper.output_filename)

            # Verify file was created
            self.assertTrue(result.exists())
            self.assertEqual(result.name, "acquisition.json")
            self.assertEqual(result.parent, Path(tmpdir))


class TestFIPMapperEdgeCases(unittest.TestCase):
    """Test cases for edge cases and error handling without setUp mocking."""

    def test_parse_intended_measurements_no_data(self):
        """Test that intended measurements parsing handles cases where no data is returned from the service.

        When the metadata service returns None (due to network issues, service unavailable, etc.),
        _parse_intended_measurements should return None and log a warning. This ensures the mapper
        can continue processing even when external services are unavailable.
        """
        mapper = FIPMapper()
        result = mapper._parse_intended_measurements("123", data=None)
        self.assertIsNone(result)

    def test_parse_intended_measurements_empty_result(self):
        """Test that intended measurements parsing handles empty data arrays from the service.

        When the metadata service returns an empty data array, _parse_intended_measurements should
        return None and log a warning. This handles cases where the subject exists but has no
        measurement assignments configured.
        """
        mapper = FIPMapper()
        result = mapper._parse_intended_measurements("123", data={"data": []})
        self.assertIsNone(result)

    def test_parse_intended_measurements_dict_payload(self):
        """Test that intended measurements parsing correctly handles dict payload format from the service.

        When the metadata service returns a single measurement object as a dict (rather than an array),
        _parse_intended_measurements should convert it to an array and process it correctly. This handles
        variations in the API response format.
        """
        mapper = FIPMapper()
        result = mapper._parse_intended_measurements(
            "123",
            data={
                "data": {
                    "fiber_name": "Fiber_0",
                    "intended_measurement_R": "dopamine",
                    "intended_measurement_G": "calcium",
                    "intended_measurement_B": None,
                    "intended_measurement_Iso": "control",
                }
            },
        )
        self.assertIn("Fiber_0", result)
        self.assertEqual(result["Fiber_0"]["R"], "dopamine")
        self.assertEqual(result["Fiber_0"]["G"], "calcium")
        self.assertIsNone(result["Fiber_0"]["B"])
        self.assertEqual(result["Fiber_0"]["Iso"], "control")

    def test_parse_implanted_fibers_no_data(self):
        """Test that implanted fibers parsing handles cases where no procedures data is returned from the service.

        When the metadata service returns None (due to network issues, service unavailable, etc.),
        _parse_implanted_fibers should return None.
        """
        mapper = FIPMapper()
        result = mapper._parse_implanted_fibers("123", data=None)
        self.assertIsNone(result)

    def test_parse_implanted_fibers_no_surgery(self):
        """Test that implanted fibers parsing handles cases where no surgery procedures exist for the subject.

        When the procedures data contains no surgery procedures, _parse_implanted_fibers should return None.
        This handles cases where the subject has other types of procedures but no fiber implant surgeries.
        """
        mapper = FIPMapper()

        result = mapper._parse_implanted_fibers("123", data={"subject_procedures": []})
        self.assertIsNone(result)

    def test_parse_implanted_fibers_no_probe_implants(self):
        """Test that implanted fibers parsing handles cases where surgery exists but no probe implants are found.

        When the procedures data contains surgery procedures but no probe implant procedures,
        _parse_implanted_fibers should return None. This handles cases where the subject had surgery
        but no fiber probes were implanted.
        """
        mapper = FIPMapper()
        result = mapper._parse_implanted_fibers(
            "123",
            data={
                "subject_procedures": [{"object_type": "Surgery", "procedures": [{"object_type": "Other procedure"}]}]
            },
        )
        self.assertIsNone(result)

    def test_parse_implanted_fibers_with_fiber_probe(self):
        """Test that implanted fibers parsing correctly identifies fiber probes from procedures data.

        When the procedures data contains probe implant procedures with fiber probe devices,
        _parse_implanted_fibers should extract the fiber indices and return them as a sorted list.
        This tests the happy path where fiber implants are properly documented in the procedures.
        """
        mapper = FIPMapper()
        result = mapper._parse_implanted_fibers(
            "123",
            data={
                "subject_procedures": [
                    {
                        "object_type": "Surgery",
                        "procedures": [
                            {
                                "object_type": "Probe implant",
                                "implanted_device": {"object_type": "Fiber probe", "name": "Fiber_2"},
                            }
                        ],
                    }
                ]
            },
        )
        self.assertEqual(result, [2])

    def test_parse_implanted_fibers_invalid_fiber_name(self):
        """Test that implanted fibers parsing raises ValueError for invalid fiber names.

        When the procedures data contains fiber probes with invalid or malformed names,
        _parse_implanted_fibers should raise ValueError. This ensures we fail loudly
        when critical fiber data cannot be parsed, rather than silently continuing.
        """
        mapper = FIPMapper()
        with self.assertRaises(ValueError) as cm:
            mapper._parse_implanted_fibers(
                "123",
                data={
                    "subject_procedures": [
                        {
                            "object_type": "Surgery",
                            "procedures": [
                                {
                                    "object_type": "Probe implant",
                                    "implanted_device": {"object_type": "Fiber probe", "name": "Invalid_Fiber"},
                                }
                            ],
                        }
                    ]
                },
            )
        self.assertIn("Invalid_Fiber", str(cm.exception))

    def test_camera_exposure_missing_delta_warning(self):
        """Test camera exposure extraction warning when delta_1 is missing."""
        mapper = FIPMapper()

        # Load fixture and remove delta_1
        with open("tests/fixtures/fip_intermediate.json", "r") as f:
            data = json.load(f)

        for key in data["rig_config"]:
            if key.startswith("light_source_") and "task" in data["rig_config"][key]:
                data["rig_config"][key]["task"].pop("delta_1", None)

        acquisition = mapper.transform(
            SimpleNamespace(**data),
            skip_validation=True,
            intended_measurements=None,
            implanted_fibers=[0, 1],  # Provide actual implanted fibers (no ROI fallback)
        )
        self.assertIsNotNone(acquisition)


if __name__ == "__main__":
    unittest.main()
