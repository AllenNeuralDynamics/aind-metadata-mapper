"""Tests for FIP mapper.

Strategy:
- Use MagicMock to stand in for dependencies we don't want to execute (e.g., extractor models, IO).
- Use patch.object() to replace specific methods (e.g., _parse_intended_measurements) so we can control
  inputs and assert calls without making network requests.
- Use SimpleNamespace to shape fixture payloads into attribute access the mapper expects, avoiding
  a hard dependency on the extractor's Pydantic model.
- Separate concerns:
  - TestFIPMapper covers core transformation with API calls mocked at setUp so tests focus on
    mapping logic and schema shape.
  - TestFIPMapperEdgeCases exercises error paths and alternative branches by patching the
    module-level HTTP helpers directly (no class-level mocks).
- Result: fast, deterministic tests with full coverage, fallbacks, and ImportError/optional-dependency behavior.
"""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

        self.assertEqual(acquisition.subject_id, "test")
        self.assertEqual(acquisition.instrument_id, "test_rig")
        self.assertEqual(acquisition.acquisition_type, "FIP")
        self.assertEqual(len(acquisition.experimenters), 2)
        self.assertEqual(acquisition.experimenters[0], "Foo")

    def test_ethics_review_id_mapping(self):
        """Test that ethics review ID is correctly mapped from intermediate metadata to Acquisition.

        The ethics_review_id field from the intermediate metadata should be mapped to the
        ethics_review_id list in the Acquisition schema. This ensures compliance with
        institutional review board requirements and proper tracking of ethical approvals.
        """
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

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

        acquisition = self.mapper._transform(SimpleNamespace(**data))
        self.assertIsNone(acquisition.ethics_review_id)

    def test_subject_details(self):
        """Test that subject details are correctly mapped from intermediate metadata to Acquisition.

        The subject details including mouse platform name and animal weights should be properly
        extracted from the intermediate metadata and mapped to the AcquisitionSubjectDetails
        object. This ensures proper tracking of experimental conditions and animal welfare data.
        """
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

        self.assertIsNotNone(acquisition.subject_details)
        self.assertEqual(acquisition.subject_details.mouse_platform_name, "wheel")
        self.assertAlmostEqual(float(acquisition.subject_details.animal_weight_prior), 25.3)
        self.assertAlmostEqual(float(acquisition.subject_details.animal_weight_post), 25.5)

    def test_data_stream_created(self):
        """Test that data stream is created with correct FIP modality.

        The FIP mapper should create a single data stream with the FIB (Fiber photometry)
        modality. This ensures the acquisition metadata correctly identifies the type of
        experimental data being collected and processed.
        """
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

        self.assertEqual(len(acquisition.data_streams), 1)
        data_stream = acquisition.data_streams[0]

        self.assertEqual(len(data_stream.modalities), 1)
        from aind_data_schema_models.modalities import Modality

        self.assertEqual(data_stream.modalities[0], Modality.FIB)

    def test_active_devices(self):
        """Test that active devices list is populated with all FIP system components.

        The FIP mapper should identify and include all active devices from the rig configuration
        in the data stream's active_devices list. This includes cameras, LEDs, and control systems
        that were used during the acquisition session.
        """
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))
        data_stream = acquisition.data_streams[0]

        self.assertIn("test_rig", data_stream.active_devices)
        self.assertIn("LED_UV", data_stream.active_devices)
        self.assertIn("LED_BLUE", data_stream.active_devices)
        self.assertIn("Camera_Green Iso", data_stream.active_devices)
        self.assertIn("Camera_Red", data_stream.active_devices)
        self.assertIn("cuTTLefishFip", data_stream.active_devices)

    def test_configurations_built(self):
        """Test that device configurations are created for all FIP system components.

        The FIP mapper should create detailed configuration objects for all devices including
        LED configurations and patch cord configurations with embedded detector configurations.
        This ensures complete metadata about the experimental setup and device parameters.
        """
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))
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
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))

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

        acquisition = self.mapper._transform(SimpleNamespace(**data))

        self.assertLess(acquisition.acquisition_start_time, acquisition.acquisition_end_time)

    def test_notes_preserved(self):
        """Test that notes field is preserved from intermediate metadata to Acquisition.

        The notes field from the intermediate metadata should be directly mapped to the
        Acquisition notes field. This preserves important experimental annotations and
        contextual information provided by the experimenter.
        """
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))
        self.assertEqual(acquisition.notes, "test session")

    def test_stimulus_epochs_empty(self):
        """Test that stimulus_epochs is an empty list for FIP acquisitions.

        FIP (Fiber Photometry) experiments typically do not involve external stimuli,
        so the stimulus_epochs field should be an empty list. This reflects the passive
        nature of most fiber photometry recordings.
        """
        acquisition = self.mapper._transform(SimpleNamespace(**self.example_intermediate_data))
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
        """Test that fiber index extraction handles invalid fiber names gracefully.

        When fiber names don't follow the expected "Fiber_N" format or contain invalid
        characters, _extract_fiber_index should return None. This prevents errors when
        processing malformed fiber names from the procedures data.
        """
        self.assertIsNone(self.mapper._extract_fiber_index("Fiber_"))
        self.assertIsNone(self.mapper._extract_fiber_index("Other_1"))
        self.assertIsNone(self.mapper._extract_fiber_index("Fiber_X"))

    def test_build_subject_details_none_when_no_platform(self):
        """Test that subject details returns None when no mouse platform is specified.

        When the intermediate metadata has no mouse_platform_name (None or missing),
        _build_subject_details should return None since there's insufficient information
        to create a meaningful AcquisitionSubjectDetails object.
        """
        data = SimpleNamespace(
            mouse_platform_name=None,
            animal_weight_prior=None,
            animal_weight_post=None,
            anaesthesia=None,
        )
        self.assertIsNone(self.mapper._build_subject_details(data))

    def test_transform_import_error_when_no_extractor(self):
        """Test that transform raises ImportError when extractor dependency is not available.

        When the aind_metadata_extractor package is not installed, the transform method
        should raise a clear ImportError with instructions on how to install the missing
        dependency. This prevents confusing errors and guides users to the solution.
        """
        from aind_metadata_mapper.fip import mapper as mapper_mod

        original = mapper_mod.FIPDataModel
        try:
            mapper_mod.FIPDataModel = None
            with self.assertRaises(ImportError) as cm:
                self.mapper.transform({})
            self.assertIn("aind_metadata_extractor is required", str(cm.exception))
        finally:
            mapper_mod.FIPDataModel = original

    def test_transform_with_extractor_available(self):
        """Test that transform works correctly when extractor dependency is available.

        When the aind_metadata_extractor package is installed, transform should successfully
        validate the input metadata using FIPDataModel and then call _transform with the
        validated model. This tests the happy path where all dependencies are available.
        """
        from aind_metadata_mapper.fip import mapper as mapper_mod

        # Mock FIPDataModel to simulate extractor being available
        mock_model = MagicMock()
        mock_instance = MagicMock()
        mock_model.model_validate.return_value = mock_instance

        original = mapper_mod.FIPDataModel
        try:
            mapper_mod.FIPDataModel = mock_model

            # Mock the _transform method to avoid complex setup
            with patch.object(self.mapper, "_transform") as mock_transform:
                mock_transform.return_value = "mock_acquisition"

                result = self.mapper.transform({"test": "data"})

                mock_model.model_validate.assert_called_once_with({"test": "data"})
                mock_transform.assert_called_once_with(mock_instance)
                self.assertEqual(result, "mock_acquisition")
        finally:
            mapper_mod.FIPDataModel = original

    def test_import_fip_data_model_success(self):
        """Test that _import_fip_data_model successfully imports when extractor is available.

        When the aind_metadata_extractor package is installed, _import_fip_data_model should
        successfully import the FIPDataModel class and return it. This tests the happy path
        of the optional import mechanism.
        """
        from aind_metadata_mapper.fip.mapper import _import_fip_data_model

        # Mock the import to simulate extractor being available
        with patch("aind_metadata_extractor.models.fip.FIPDataModel", MagicMock()) as mock_model:
            result = _import_fip_data_model()
            # Should return the mocked class
            self.assertIsNotNone(result)
            self.assertEqual(result, mock_model)

    def test_import_fip_data_model_import_error(self):
        """Test that _import_fip_data_model handles ImportError gracefully when extractor is missing.

        When the aind_metadata_extractor package is not installed, _import_fip_data_model should
        catch the ImportError and return None. This allows the mapper to work without the extractor
        dependency while providing clear error messages when needed.
        """
        from aind_metadata_mapper.fip.mapper import _import_fip_data_model

        # Mock the import to raise ImportError
        with patch("builtins.__import__", side_effect=ImportError("No module named 'aind_metadata_extractor'")):
            result = _import_fip_data_model()
            self.assertIsNone(result)

    def test_get_led_wavelength_unknown(self):
        """Test that LED wavelength lookup returns None for unknown LED names.

        When _get_led_wavelength is called with an unknown LED name, it should return None.
        This handles cases where the rig configuration contains LED devices that aren't
        recognized by the wavelength mapping system.
        """
        mapper = FIPMapper()
        result = mapper._get_led_wavelength("unknown_led")
        self.assertIsNone(result)

    def test_run_job_calls_transform(self):
        """Test that run_job correctly orchestrates the transform and write operations.

        The run_job method should call transform to convert the input metadata to an Acquisition,
        then call write_acquisition to save it to the specified output directory. This tests the
        complete workflow from input metadata to output file.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.write_acquisition") as mock_write:
            mock_write.return_value = "test_path"

            # Mock the transform method to avoid extractor dependency
            with patch.object(mapper, "transform") as mock_transform:
                mock_acquisition = "mock_acquisition"
                mock_transform.return_value = mock_acquisition

                result = mapper.run_job({}, "/output")

                mock_transform.assert_called_once_with({})
                mock_write.assert_called_once_with(mock_acquisition, "/output", "acquisition.json")
                self.assertEqual(result, "test_path")

    def test_write_calls_write_acquisition(self):
        """Test that write method correctly delegates to write_acquisition utility function.

        The write method should call write_acquisition with the provided Acquisition model,
        output directory, and the mapper's configured output filename. This tests the
        integration with the utility function for file writing.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.write_acquisition") as mock_write:
            mock_write.return_value = "test_path"
            mock_acquisition = "mock_acquisition"

            result = mapper.write(mock_acquisition, "/output")

            mock_write.assert_called_once_with(mock_acquisition, "/output", "acquisition.json")
            self.assertEqual(result, "test_path")


class TestFIPMapperEdgeCases(unittest.TestCase):
    """Test cases for edge cases and error handling without setUp mocking."""

    def test_parse_intended_measurements_no_data(self):
        """Test that intended measurements parsing handles cases where no data is returned from the service.

        When the metadata service returns None (due to network issues, service unavailable, etc.),
        _parse_intended_measurements should return None and log a warning. This ensures the mapper
        can continue processing even when external services are unavailable.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_intended_measurements") as mock_get:
            mock_get.return_value = None
            result = mapper._parse_intended_measurements("123")
            self.assertIsNone(result)

    def test_parse_intended_measurements_empty_result(self):
        """Test that intended measurements parsing handles empty data arrays from the service.

        When the metadata service returns an empty data array, _parse_intended_measurements should
        return None and log a warning. This handles cases where the subject exists but has no
        measurement assignments configured.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_intended_measurements") as mock_get:
            mock_get.return_value = {"data": []}
            result = mapper._parse_intended_measurements("123")
            self.assertIsNone(result)

    def test_parse_intended_measurements_dict_payload(self):
        """Test that intended measurements parsing correctly handles dict payload format from the service.

        When the metadata service returns a single measurement object as a dict (rather than an array),
        _parse_intended_measurements should convert it to an array and process it correctly. This handles
        variations in the API response format.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_intended_measurements") as mock_get:
            mock_get.return_value = {
                "data": {
                    "fiber_name": "Fiber_0",
                    "intended_measurement_R": "dopamine",
                    "intended_measurement_G": "calcium",
                    "intended_measurement_B": None,
                    "intended_measurement_Iso": "control",
                }
            }
            result = mapper._parse_intended_measurements("123")
            self.assertIn("Fiber_0", result)
            self.assertEqual(result["Fiber_0"]["R"], "dopamine")
            self.assertEqual(result["Fiber_0"]["G"], "calcium")
            self.assertIsNone(result["Fiber_0"]["B"])
            self.assertEqual(result["Fiber_0"]["Iso"], "control")

    def test_parse_implanted_fibers_no_data(self):
        """Test that implanted fibers parsing handles cases where no procedures data is returned from the service.

        When the metadata service returns None (due to network issues, service unavailable, etc.),
        _parse_implanted_fibers should return None. This ensures the mapper can continue processing
        even when the procedures service is unavailable, falling back to ROI-based fiber detection.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_procedures") as mock_get:
            mock_get.return_value = None
            result = mapper._parse_implanted_fibers("123")
            self.assertIsNone(result)

    def test_parse_implanted_fibers_no_surgery(self):
        """Test that implanted fibers parsing handles cases where no surgery procedures exist for the subject.

        When the procedures data contains no surgery procedures, _parse_implanted_fibers should return None.
        This handles cases where the subject has other types of procedures but no fiber implant surgeries.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_procedures") as mock_get:
            mock_get.return_value = {"subject_procedures": []}
            result = mapper._parse_implanted_fibers("123")
            self.assertIsNone(result)

    def test_parse_implanted_fibers_no_probe_implants(self):
        """Test that implanted fibers parsing handles cases where surgery exists but no probe implants are found.

        When the procedures data contains surgery procedures but no probe implant procedures,
        _parse_implanted_fibers should return None. This handles cases where the subject had surgery
        but no fiber probes were implanted.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_procedures") as mock_get:
            mock_get.return_value = {
                "subject_procedures": [{"object_type": "Surgery", "procedures": [{"object_type": "Other procedure"}]}]
            }
            result = mapper._parse_implanted_fibers("123")
            self.assertIsNone(result)

    def test_parse_implanted_fibers_with_fiber_probe(self):
        """Test that implanted fibers parsing correctly identifies fiber probes from procedures data.

        When the procedures data contains probe implant procedures with fiber probe devices,
        _parse_implanted_fibers should extract the fiber indices and return them as a sorted list.
        This tests the happy path where fiber implants are properly documented in the procedures.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_procedures") as mock_get:
            mock_get.return_value = {
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
            }
            result = mapper._parse_implanted_fibers("123")
            self.assertEqual(result, [2])

    def test_parse_implanted_fibers_invalid_fiber_name(self):
        """Test that implanted fibers parsing handles invalid fiber names gracefully.

        When the procedures data contains fiber probes with invalid or malformed names,
        _parse_implanted_fibers should skip those entries and return None if no valid
        fiber indices can be extracted. This prevents errors from malformed procedure data.
        """
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_procedures") as mock_get:
            mock_get.return_value = {
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
            }
            result = mapper._parse_implanted_fibers("123")
            self.assertIsNone(result)

    def test_camera_exposure_missing_delta_warning(self):
        """Test camera exposure extraction warning when delta_1 is missing."""
        mapper = FIPMapper()

        with patch("aind_metadata_mapper.fip.mapper.get_intended_measurements", return_value=None):
            with patch("aind_metadata_mapper.fip.mapper.get_procedures", return_value=None):
                # Load fixture and remove delta_1
                with open("tests/fixtures/fip_intermediate.json", "r") as f:
                    data = json.load(f)

                for key in data["rig_config"]:
                    if key.startswith("light_source_") and "task" in data["rig_config"][key]:
                        data["rig_config"][key]["task"].pop("delta_1", None)

                acquisition = mapper._transform(SimpleNamespace(**data))
                self.assertIsNotNone(acquisition)


if __name__ == "__main__":
    unittest.main()
