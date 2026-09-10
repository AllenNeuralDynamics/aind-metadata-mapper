"""Tests for FIP mapper.

Strategy:
- Use dependency injection to pass test data directly instead of mocking.
- Use SimpleNamespace to shape fixture payloads into attribute access the mapper expects.
- Pass intended_measurements and implanted_fibers as parameters to avoid network calls.
- Use skip_validation=True to avoid FIPDataModel dependency in tests.
- get_iacuc_protocol is mocked in setUp because transform always calls it to verify
  ethics_review_id against LabTracks; all other collaborators use dependency injection.
"""

import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.fip import mapper as mapper_mod
from aind_metadata_mapper.fip.constants import VR_FORAGING_FIP_REPO_URL
from aind_metadata_mapper.fip.mapper import FIPMapper


class TestFIPMapper(unittest.TestCase):
    """Test cases for FIPMapper."""

    def setUp(self):
        """Set up test fixtures."""
        self.mapper = FIPMapper()
        # Load fixture of example intermediate data (already in nested structure)
        with open("tests/fixtures/fip_intermediate.json", "r", encoding="utf-8") as f:
            self.example_intermediate_data = json.load(f)

        # Test data for dependency injection
        self.test_intended_measurements = {
            "Fiber_0": {"R": "jRCaMP1b", "G": "dLight", "B": None, "Iso": "dLight"},
            "Fiber_1": {"R": "jRCaMP1b", "G": "dLight", "B": None, "Iso": "dLight"},
        }
        self.test_implanted_fibers = [0, 1]  # Two implanted fibers
        self.test_ethics_review_id = ["2414"]  # Injected to keep tests network-free

        # transform always calls get_iacuc_protocol to verify ethics_review_id against
        # LabTracks, so mock it here to keep all tests network-free. Individual tests
        # override the return value (or re-patch) to exercise specific behaviors.
        patcher = unittest.mock.patch.object(mapper_mod, "get_iacuc_protocol", return_value="2414")
        self.mock_get_iacuc_protocol = patcher.start()
        self.addCleanup(patcher.stop)

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

        The transform method should correctly extract and map the essential fields from the
        intermediate FIP metadata into the Acquisition schema. This includes subject ID, instrument ID,
        acquisition type, experimenter information, notes, and other basic fields.
        """
        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
        )

        self.assertEqual(acquisition.subject_id, "test")
        self.assertEqual(acquisition.instrument_id, "test_rig")
        self.assertEqual(acquisition.acquisition_type, "")
        self.assertEqual(len(acquisition.experimenters), 2)
        self.assertEqual(acquisition.experimenters[0], "Foo")
        self.assertEqual(acquisition.notes, "test session")
        self.assertIsNone(acquisition.subject_details)
        self.assertEqual(len(acquisition.stimulus_epochs), 0)

    def test_ethics_review_id_injected(self):
        """Test that an explicitly provided ethics_review_id is verified and mapped through.

        When ethics_review_id is passed to transform and matches the IACUC protocol in
        LabTracks, it should be set on the Acquisition.
        """
        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=[0, 1],
            ethics_review_id=["2414"],
        )

        self.mock_get_iacuc_protocol.assert_called_once_with("test")
        self.assertEqual(acquisition.ethics_review_id, ["2414"])

    def test_ethics_review_id_looked_up_from_labtracks(self):
        """Test that ethics_review_id is looked up by subject_id when not provided.

        When ethics_review_id is None, transform should fetch the IACUC protocol from
        LabTracks via get_iacuc_protocol and wrap the single value in a list.
        """
        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=[0, 1],
        )

        self.mock_get_iacuc_protocol.assert_called_once_with("test")
        self.assertEqual(acquisition.ethics_review_id, ["2414"])

    def test_ethics_review_id_none_when_not_found(self):
        """Test that ethics_review_id is left unmapped when LabTracks has no protocol.

        When get_iacuc_protocol returns None, ethics_review_id should be None so it can
        be filled from acquisition_behavior.json when acquisitions are merged downstream.
        """
        self.mock_get_iacuc_protocol.return_value = None

        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=[0, 1],
        )

        self.assertIsNone(acquisition.ethics_review_id)

    def test_ethics_review_id_mismatch_raises(self):
        """Test that a provided ethics_review_id that mismatches LabTracks raises.

        LabTracks is the source of truth for the IACUC protocol, so a provided
        ethics_review_id that disagrees with it should raise a ValueError (which the
        GatherMetadataJob gates behind the raise_if_mapper_errors setting).
        """
        with self.assertRaises(ValueError) as context:
            self.mapper.transform(
                self.example_intermediate_data,
                skip_validation=True,
                intended_measurements=self.test_intended_measurements,
                implanted_fibers=[0, 1],
                ethics_review_id=["9999"],
            )

        self.assertIn("does not match the IACUC protocol", str(context.exception))

    def test_ethics_review_id_kept_when_unverifiable(self):
        """Test that a provided ethics_review_id is kept when LabTracks has no protocol.

        When get_iacuc_protocol returns None, a provided value cannot be verified, so it
        should be used as-is (with a warning) rather than raising.
        """
        self.mock_get_iacuc_protocol.return_value = None

        with self.assertLogs(mapper_mod.logger, level="WARNING") as logs:
            acquisition = self.mapper.transform(
                self.example_intermediate_data,
                skip_validation=True,
                intended_measurements=self.test_intended_measurements,
                implanted_fibers=[0, 1],
                ethics_review_id=["2414"],
            )

        self.assertEqual(acquisition.ethics_review_id, ["2414"])
        self.assertTrue(any("Could not verify" in msg for msg in logs.output))

    def test_data_stream_created(self):
        """Test that data stream is created with correct FIP modality.

        The FIP mapper should create a single data stream with the FIB (Fiber photometry)
        modality. This ensures the acquisition metadata correctly identifies the type of
        experimental data being collected and processed.
        """
        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
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
            "rig": self.example_intermediate_data["rig"],
        }

        acquisition = self.mapper.transform(
            test_metadata,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
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
        # Use fixture data which has commit_hash=None
        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
        )
        data_stream = acquisition.data_streams[0]
        # Verify code field is None (fixture has commit_hash=None)
        self.assertIsNone(data_stream.code)

    def test_acquisition_type_passed_through(self):
        """Test that a provided acquisition_type is used verbatim."""
        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
            acquisition_type="AindVrForaging",
        )
        self.assertEqual(acquisition.acquisition_type, "AindVrForaging")

    def test_acquisition_type_defaults_to_empty_string(self):
        """Test that an unset acquisition_type becomes an empty string, not None.

        acquisition_type is required by aind-data-schema, so None is not a valid
        value. An empty string is left to be filled by the acquisition merge
        downstream, the same way ethics_review_id is.
        """
        for value in (None, ""):
            with self.subTest(acquisition_type=value):
                acquisition = self.mapper.transform(
                    self.example_intermediate_data,
                    skip_validation=True,
                    intended_measurements=self.test_intended_measurements,
                    implanted_fibers=self.test_implanted_fibers,
                    ethics_review_id=self.test_ethics_review_id,
                    acquisition_type=value,
                )
                self.assertEqual(acquisition.acquisition_type, "")
                self.assertIsNotNone(acquisition.acquisition_type)

    def test_active_devices(self):
        """Test that active devices list is populated with all FIP system components.

        The FIP mapper should identify and include all active devices from the rig configuration
        in the data stream's active_devices list. This includes cameras, LEDs, and control systems
        that were used during the acquisition session.
        """
        acquisition = self.mapper.transform(
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
        )
        data_stream = acquisition.data_streams[0]

        # Verify that active devices list is populated
        self.assertGreater(len(data_stream.active_devices), 0)

        # Verify that devices from rig config are present (using transformation)
        rig_config = self.example_intermediate_data["rig"]
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
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
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
            self.example_intermediate_data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
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
        data["data_stream_metadata"][0]["start_time"] = "2025-07-18T13:00:00-07:00"
        data["data_stream_metadata"][0]["end_time"] = "2025-07-18T12:00:00-07:00"

        acquisition = self.mapper.transform(
            data,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
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
            "rig": self.example_intermediate_data["rig"],
        }

        acquisition = self.mapper.transform(
            test_metadata,
            skip_validation=True,
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
        )

        # Verify that the acquisition uses the earliest start (t0) and latest end (t5)
        expected_start = t0
        expected_end = t5

        self.assertEqual(acquisition.acquisition_start_time, expected_start)
        self.assertEqual(acquisition.acquisition_end_time, expected_end)
        self.assertEqual(acquisition.data_streams[0].stream_start_time, expected_start)
        self.assertEqual(acquisition.data_streams[0].stream_end_time, expected_end)

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
        schema_compliant_data = self.example_intermediate_data.copy()

        # Mock _parse_implanted_fibers to return (None, False) (simulating service failure)
        original_method = self.mapper._parse_implanted_fibers
        self.mapper._parse_implanted_fibers = lambda subject_id, data=None: (None, False)

        try:
            with self.assertRaises(ValueError) as cm:
                self.mapper.transform(
                    schema_compliant_data,
                    skip_validation=True,
                    intended_measurements=self.test_intended_measurements,
                    implanted_fibers=None,  # Will trigger fetch, returns None
                    ethics_review_id=["2414"],
                )
            self.assertIn("Failed to retrieve procedures data", str(cm.exception))
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
                ethics_review_id=["2414"],
            )
        self.assertIn("No implanted fibers found", str(cm.exception))

    def test_transform_missing_implanted_fibers_in_procedures(self):
        """Test that transform provides a distinct error when procedures are fetched but no implanted fibers are found.

        This ensures we distinguish between "service failed" and "no fibers in procedures data".
        """
        schema_compliant_data = self.example_intermediate_data.copy()

        # Mock _parse_implanted_fibers to return (None, True) (procedures fetched, but no fibers found)
        original_method = self.mapper._parse_implanted_fibers
        self.mapper._parse_implanted_fibers = lambda subject_id, data=None: (None, True)

        try:
            with self.assertRaises(ValueError) as cm:
                self.mapper.transform(
                    schema_compliant_data,
                    skip_validation=True,
                    intended_measurements=self.test_intended_measurements,
                    implanted_fibers=None,  # Will trigger fetch, returns (None, True)
                    ethics_review_id=["2414"],
                )
            self.assertIn("No implanted fibers found in procedures data", str(cm.exception))
            self.assertNotIn("Failed to retrieve procedures data", str(cm.exception))
        finally:
            # Restore original method
            self.mapper._parse_implanted_fibers = original_method

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

    def test_transform_calls_validation_when_not_skipped(self):
        """Test that transform calls validation when skip_validation=False.

        This test exercises the validation code path. Uses nested structure
        that matches the expected schema format.
        """
        schema_compliant_data = self.example_intermediate_data.copy()
        # Remove id to test the code path that adds it
        for stream in schema_compliant_data["data_stream_metadata"]:
            stream.pop("id", None)
        # Ensure data_stream_metadata items have id (required by schema)
        for stream in schema_compliant_data["data_stream_metadata"]:
            if "id" not in stream:
                stream["id"] = "test_stream"

        result = self.mapper.transform(
            schema_compliant_data,
            skip_validation=False,  # Don't skip - will validate against schema
            intended_measurements=self.test_intended_measurements,
            implanted_fibers=self.test_implanted_fibers,
            ethics_review_id=self.test_ethics_review_id,
        )
        # Should complete successfully (validation passes)
        self.assertIsNotNone(result)

    def test_run_job_calls_transform(self):
        """Test that run_job correctly orchestrates the transform and write operations.

        The run_job method should call transform to convert the input metadata to an Acquisition,
        and write it using write_standard_file. This tests the complete workflow from input metadata to output file.
        """
        mapper = FIPMapper()

        from aind_metadata_mapper.base import MapperJobSettings

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create input file
            input_file = Path(tmpdir) / "fip.json"
            with open("tests/fixtures/fip_intermediate.json", "r", encoding="utf-8") as f:
                test_data = json.load(f)
            with open(input_file, "w") as f:
                json.dump(test_data, f)

            # Create job settings
            job_settings = MapperJobSettings(
                input_filepath=input_file,
                output_directory=Path(tmpdir),
                output_filename_suffix="fip",
            )

            # Mock the metadata service calls to avoid network requests
            import unittest.mock

            with (
                unittest.mock.patch.object(
                    mapper, "_parse_intended_measurements", return_value=self.test_intended_measurements
                ),
                unittest.mock.patch.object(
                    mapper, "_parse_implanted_fibers", return_value=(self.test_implanted_fibers, True)
                ),
                unittest.mock.patch.object(mapper, "_validate_fip_metadata"),
                unittest.mock.patch.object(mapper_mod, "get_iacuc_protocol", return_value="2414"),
            ):
                mapper.run_job(job_settings)

            # write_standard_file creates acquisition_fip.json (with .json extension)
            expected_file = Path(tmpdir) / "acquisition_fip.json"
            self.assertTrue(
                expected_file.exists(), f"Expected {expected_file} but found {list(Path(tmpdir).iterdir())}"
            )

            # Verify it's valid JSON
            with open(expected_file, "r") as f:
                data = json.load(f)
            self.assertIn("subject_id", data)


class TestFIPMapperEdgeCases(unittest.TestCase):
    """Test cases for edge cases and error handling without setUp mocking."""

    def test_parse_intended_measurements_no_data(self):
        """Test that intended measurements parsing handles cases where no data is returned from the service.

        When the metadata service returns None or empty data, _parse_intended_measurements should
        return None and log a warning. This ensures the mapper can continue processing even when
        external services are unavailable.
        """
        mapper = FIPMapper()
        # No data
        result = mapper._parse_intended_measurements("123", data=None)
        self.assertIsNone(result)
        # Empty data array
        result = mapper._parse_intended_measurements("123", data={"data": []})
        self.assertIsNone(result)
        # Data with no valid fiber names
        result = mapper._parse_intended_measurements("123", data={"data": [{"fiber_name": None}]})
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
        _parse_implanted_fibers should return (None, False).
        """
        mapper = FIPMapper()
        result, procedures_fetched = mapper._parse_implanted_fibers("123", data=None)
        self.assertIsNone(result)
        self.assertFalse(procedures_fetched)

    def test_parse_implanted_fibers_no_fibers_found(self):
        """Test that implanted fibers parsing returns (None, True) when no fiber probes are found.

        This covers cases where procedures data is empty, has no surgery, or surgery has no probe implants.
        """
        mapper = FIPMapper()

        # No procedures
        result, procedures_fetched = mapper._parse_implanted_fibers("123", data={"subject_procedures": []})
        self.assertIsNone(result)
        self.assertTrue(procedures_fetched)

        # Surgery but no probe implants
        result, procedures_fetched = mapper._parse_implanted_fibers(
            "123",
            data={
                "subject_procedures": [{"object_type": "Surgery", "procedures": [{"object_type": "Other procedure"}]}]
            },
        )
        self.assertIsNone(result)
        self.assertTrue(procedures_fetched)

    def test_parse_implanted_fibers_with_fiber_probe(self):
        """Test that implanted fibers parsing correctly identifies fiber probes from procedures data.

        When the procedures data contains probe implant procedures with fiber probe devices,
        _parse_implanted_fibers should extract the fiber indices and return them as a sorted list.
        This tests the happy path where fiber implants are properly documented in the procedures.
        """
        mapper = FIPMapper()
        result, procedures_fetched = mapper._parse_implanted_fibers(
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
        self.assertTrue(procedures_fetched)

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

        for key in data["rig"]:
            if key.startswith("light_source_") and "task" in data["rig"][key]:
                data["rig"][key]["task"].pop("delta_1", None)

        acquisition = mapper.transform(
            data,
            skip_validation=True,
            intended_measurements=None,
            implanted_fibers=[0, 1],  # Provide actual implanted fibers (no ROI fallback)
            ethics_review_id=["2414"],
        )
        self.assertIsNotNone(acquisition)


if __name__ == "__main__":
    unittest.main()
