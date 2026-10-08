"""Tests for mesoscope mapper."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.stimulus_modality import StimulusModality

from aind_metadata_mapper.base import MapperJobSettings
from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.mapper_registry import registry
from aind_metadata_mapper.mesoscope.mapper import MesoscopeMapper
from aind_metadata_mapper.models import DataDescriptionSettings, JobSettings

TEST_DIR = Path(__file__).resolve().parents[1]
FIXTURE_PATH = TEST_DIR / "fixtures" / "mesoscope_intermediate.json"


class TestMesoscopeMapper(unittest.TestCase):
    """Unit tests for MesoscopeMapper."""

    def setUp(self) -> None:
        """Load fixture data."""
        self.mapper = MesoscopeMapper()
        with open(FIXTURE_PATH, "r", encoding="utf-8") as fixture_stream:
            self.fixture = json.load(fixture_stream)

    def test_registry_registration(self) -> None:
        """Mesoscope mapper should be registered for GatherMetadataJob."""
        self.assertIs(registry["mesoscope"], MesoscopeMapper)

    def test_transform_maps_two_coupled_planes(self) -> None:
        """Mapper should convert two-plane mesoscope data into Acquisition."""
        acquisition = self.mapper.transform(copy.deepcopy(self.fixture))

        self.assertEqual(acquisition.subject_id, "825546")
        self.assertEqual(acquisition.instrument_id, "422_MESO2_20260122")
        self.assertEqual(acquisition.acquisition_type, "STAGE_1")
        self.assertEqual(acquisition.ethics_review_id, ["2115"])
        self.assertEqual(acquisition.model_dump(mode="json")["coordinate_system"]["name"], "BREGMA_ARI")
        self.assertEqual(acquisition.subject_details.mouse_platform_name, "disc")
        self.assertEqual(acquisition.subject_details.reward_consumed_unit, "milliliter")

        stream = acquisition.data_streams[0]
        self.assertEqual(stream.active_devices[0], "MESO.2")
        self.assertIn("Mesoscope", stream.active_devices)
        self.assertIn("Behavior camera assembly", stream.active_devices)
        self.assertIn("Eye camera assembly", stream.active_devices)
        self.assertIn("Face camera assembly", stream.active_devices)
        self.assertIn("magnification=16x", stream.notes)
        self.assertIn("scanimage_roi_index=0", stream.notes)

        imaging_config = stream.configurations[0]
        self.assertEqual(imaging_config.device_name, "MESO.2")
        self.assertEqual(imaging_config.channels[0].channel_name, "Ophys Channel")
        self.assertEqual(imaging_config.channels[0].detector.device_name, "Unknown Detector")
        self.assertEqual(imaging_config.channels[0].light_sources[0].device_name, "Laser")
        self.assertEqual(imaging_config.channels[0].light_sources[0].wavelength, 920)

        self.assertEqual(len(imaging_config.images), 2)
        first_plane = imaging_config.images[0].planes[0]
        second_plane = imaging_config.images[1].planes[0]
        self.assertEqual(first_plane.plane_index, 0)
        self.assertEqual(first_plane.coupled_plane_index, 1)
        self.assertEqual(second_plane.plane_index, 1)
        self.assertEqual(second_plane.coupled_plane_index, 0)
        self.assertEqual(first_plane.power, 88.0)
        self.assertEqual(first_plane.power_ratio, 28.0)
        self.assertEqual(first_plane.targeted_structure.acronym, "VISp")

        first_transform = imaging_config.images[0].image_to_acquisition_transform
        self.assertEqual(first_transform[0].scale, [0.00078, 0.00078])
        self.assertEqual(first_transform[1].translation, [0.0015, 0.0015, 0.0])
        self.assertEqual(imaging_config.images[0].dimensions.scale, [512.0, 512.0])

        stimulus_epoch = acquisition.stimulus_epochs[0]
        self.assertEqual(stimulus_epoch.stimulus_name, "drifting_gratings_contrast")
        self.assertEqual(stimulus_epoch.code.name, "STAGE_1")
        self.assertEqual(stimulus_epoch.code.url, "")
        self.assertEqual(stimulus_epoch.code.core_dependency.name, "camstim")
        stimulus_parameters = stimulus_epoch.code.parameters.model_dump()
        self.assertEqual(stimulus_epoch.stimulus_modalities, [StimulusModality.VISUAL])
        self.assertEqual(stimulus_parameters["stimulus_type"], "Visual Stimulation")
        self.assertIsNone(stimulus_parameters["notes"])
        self.assertEqual(
            stimulus_parameters["stimulus_parameters"]["contrast"],
            [0.2, 0.8],
        )

    def test_transform_maps_multiple_groups_in_platform_order(self) -> None:
        """Mapper should keep platform plane order across multiple groups."""
        metadata = copy.deepcopy(self.fixture)
        metadata["session_metadata"]["platform"]["imaging_plane_groups"].append(
            {
                "local_z_stack_tif": "1770928800_local_z_stack1.tiff",
                "acquisition_framerate_Hz": 42.85,
                "scanimage_power_percent": 90,
                "scanimage_split_percent": 13,
                "imaging_planes": [
                    {
                        "targeted_depth": 325,
                        "targeted_structure_id": "VISam",
                        "scanimage_roi_index": 1,
                        "registration": {"pixel_size_um": 0.78},
                    },
                    {
                        "targeted_depth": 400,
                        "targeted_structure_id": "VISpm",
                        "scanimage_roi_index": 1,
                        "registration": {"pixel_size_um": 0.78},
                    },
                ],
            }
        )

        acquisition = self.mapper.transform(metadata)
        imaging_config = acquisition.data_streams[0].configurations[0]

        self.assertEqual(len(imaging_config.images), 4)
        plane_indices = [image.planes[0].plane_index for image in imaging_config.images]
        coupled_indices = [image.planes[0].coupled_plane_index for image in imaging_config.images]
        depths = [image.planes[0].depth for image in imaging_config.images]

        self.assertEqual(plane_indices, [0, 1, 2, 3])
        self.assertEqual(coupled_indices, [1, 0, 3, 2])
        self.assertEqual(depths, [175.0, 250.0, 325.0, 400.0])
        self.assertEqual(imaging_config.images[2].planes[0].power_ratio, 13.0)

    def test_transform_uses_plane_power_when_group_power_missing(self) -> None:
        """Mapper should fall back to plane-level percent power."""
        metadata = copy.deepcopy(self.fixture)
        group = metadata["session_metadata"]["platform"]["imaging_plane_groups"][0]
        group.pop("scanimage_power_percent")
        group["imaging_planes"][0]["scanimage_power"] = 61
        group["imaging_planes"][1]["scanimage_power"] = 62

        acquisition = self.mapper.transform(metadata)
        imaging_config = acquisition.data_streams[0].configurations[0]

        self.assertEqual(imaging_config.images[0].planes[0].power, 61.0)
        self.assertEqual(imaging_config.images[1].planes[0].power, 62.0)

    def test_transform_assigns_pacific_timezone_to_naive_times(self) -> None:
        """Naive timestamps should use America/Los_Angeles."""
        metadata = copy.deepcopy(self.fixture)
        metadata["job_settings"]["session_start_time"] = "2026-02-12T13:04:36.555247"
        metadata["job_settings"]["session_end_time"] = "2026-02-12T14:07:12.672057"
        metadata["camstim_epchs"][0]["stimulus_start_time"] = "2026-02-12T13:05:03.110704"
        metadata["camstim_epchs"][0]["stimulus_end_time"] = "2026-02-12T13:19:02.807314"

        acquisition = self.mapper.transform(metadata)

        self.assertEqual(str(acquisition.acquisition_start_time.tzinfo), "America/Los_Angeles")
        self.assertEqual(str(acquisition.stimulus_epochs[0].stimulus_start_time.tzinfo), "America/Los_Angeles")

    def test_transform_raises_on_missing_instrument_id(self) -> None:
        """Mapper should fail without explicit job_settings.instrument_id."""
        metadata = copy.deepcopy(self.fixture)
        metadata["job_settings"].pop("instrument_id")

        with self.assertRaises(ValueError) as context:
            self.mapper.transform(metadata)

        self.assertIn("job_settings.instrument_id", str(context.exception))

    def test_transform_raises_on_missing_acquisition_type(self) -> None:
        """Mapper should fail without a session type."""
        metadata = copy.deepcopy(self.fixture)
        metadata["camstim_session_type"] = ""
        metadata["session_metadata"]["platform"]["stimulus_name"] = ""

        with self.assertRaises(ValueError) as context:
            self.mapper.transform(metadata)

        self.assertIn("camstim_session_type", str(context.exception))

    def test_transform_raises_when_session_times_are_inverted(self) -> None:
        """Mapper should fail when session end is before start."""
        metadata = copy.deepcopy(self.fixture)
        metadata["job_settings"]["session_start_time"] = "2026-02-12T14:07:12.672057-08:00"
        metadata["job_settings"]["session_end_time"] = "2026-02-12T13:04:36.555247-08:00"

        with self.assertRaises(ValueError) as context:
            self.mapper.transform(metadata)

        self.assertIn("session_end_time", str(context.exception))

    def test_transform_accepts_string_experimenter(self) -> None:
        """Mapper should normalize a single experimenter string."""
        metadata = copy.deepcopy(self.fixture)
        metadata["job_settings"]["experimenter_full_name"] = "Sam Seid"

        acquisition = self.mapper.transform(metadata)

        self.assertEqual(acquisition.experimenters, ["Sam Seid"])

    def test_transform_raises_on_malformed_pairing(self) -> None:
        """Mapper should fail when a plane group has more than two planes."""
        metadata = copy.deepcopy(self.fixture)
        metadata["session_metadata"]["platform"]["imaging_plane_groups"][0]["imaging_planes"].append(
            {
                "targeted_depth": 300,
                "targeted_structure_id": "VISp",
                "scanimage_roi_index": 0,
                "registration": {"pixel_size_um": 0.78},
                "scanimage_power": 70,
            }
        )

        with self.assertRaises(ValueError) as context:
            self.mapper.transform(metadata)

        self.assertIn("must contain 1 or 2 imaging_planes", str(context.exception))

    def test_transform_extends_acquisition_to_stimulus_interval(self) -> None:
        """Acquisition bounds should span stream and stimulus times."""
        metadata = copy.deepcopy(self.fixture)
        metadata["job_settings"]["session_end_time"] = "2026-02-20T11:17:49.592634-08:00"
        metadata["camstim_epchs"][0]["stimulus_start_time"] = "2026-02-20T10:07:44.569904-08:00"
        metadata["camstim_epchs"][0]["stimulus_end_time"] = "2026-02-20T11:17:55.926134-08:00"

        acquisition = self.mapper.transform(metadata)

        self.assertEqual(acquisition.acquisition_end_time.isoformat(), "2026-02-20T11:17:55.926134-08:00")
        self.assertEqual(
            acquisition.data_streams[0].stream_end_time.isoformat(),
            "2026-02-20T11:17:49.592634-08:00",
        )
        self.assertEqual(
            acquisition.stimulus_epochs[0].stimulus_end_time.isoformat(),
            "2026-02-20T11:17:55.926134-08:00",
        )

    def test_transform_rejects_inverted_stimulus_interval(self) -> None:
        """Mapper should reject inverted stimulus intervals."""
        metadata = copy.deepcopy(self.fixture)
        metadata["camstim_epchs"][0]["stimulus_start_time"] = "2026-02-12T13:19:02.807314-08:00"
        metadata["camstim_epchs"][0]["stimulus_end_time"] = "2026-02-12T13:05:03.110704-08:00"

        with self.assertRaises(ValueError) as context:
            self.mapper.transform(metadata)

        self.assertIn("stimulus_end_time", str(context.exception))

    def test_transform_defaults_missing_stimulus_modalities_to_visual(self) -> None:
        """Mapper should normalize missing mesoscope Camstim modalities to Visual."""
        metadata = copy.deepcopy(self.fixture)
        metadata["camstim_epchs"][0].pop("stimulus_modalities", None)

        acquisition = self.mapper.transform(metadata)

        self.assertEqual(acquisition.stimulus_epochs[0].stimulus_modalities, [StimulusModality.VISUAL])

    def test_transform_preserves_explicit_stimulus_modalities(self) -> None:
        """Mapper should preserve explicit upstream stimulus fields."""
        metadata = copy.deepcopy(self.fixture)
        metadata["camstim_epchs"][0]["stimulus_modalities"] = [StimulusModality.VISUAL]
        metadata["camstim_epchs"][0]["stimulus_parameters"][0]["stimulus_type"] = "Custom visual"
        metadata["camstim_epchs"][0]["stimulus_parameters"][0]["notes"] = "keep me"

        acquisition = self.mapper.transform(metadata)

        self.assertEqual(acquisition.stimulus_epochs[0].stimulus_modalities, [StimulusModality.VISUAL])
        parameters = acquisition.stimulus_epochs[0].code.parameters.model_dump()
        self.assertEqual(parameters["stimulus_type"], "Custom visual")
        self.assertEqual(parameters["notes"], "keep me")

    def test_transform_ignores_registration_pixel_size(self) -> None:
        """Baseline mapper should keep legacy default pixel scale."""
        metadata = copy.deepcopy(self.fixture)
        metadata["session_metadata"]["platform"]["imaging_plane_groups"][0]["imaging_planes"][0]["registration"][
            "pixel_size_um"
        ] = 0.4962

        acquisition = self.mapper.transform(metadata)

        scale = acquisition.data_streams[0].configurations[0].images[0].image_to_acquisition_transform[0].scale
        self.assertEqual(scale, [0.00078, 0.00078])

    def test_run_job_writes_acquisition_mesoscope_json(self) -> None:
        """run_job should write standard acquisition output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            input_path = runtime_dir / "mesoscope.json"
            with open(input_path, "w", encoding="utf-8") as output_stream:
                json.dump(self.fixture, output_stream)

            settings = MapperJobSettings(
                input_filepath=input_path,
                output_directory=runtime_dir,
                output_filename_suffix="mesoscope",
            )
            self.mapper.run_job(settings)

            output_path = runtime_dir / "acquisition_mesoscope.json"
            self.assertTrue(output_path.exists())

            with open(output_path, "r", encoding="utf-8") as input_stream:
                acquisition = Acquisition.model_validate(json.load(input_stream))
            self.assertEqual(acquisition.instrument_id, "422_MESO2_20260122")

    def test_gather_metadata_job_runs_registered_mapper_without_network(self) -> None:
        """GatherMetadataJob should discover and run mesoscope mapper."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            with open(runtime_dir / "mesoscope.json", "w", encoding="utf-8") as output_stream:
                json.dump(self.fixture, output_stream)

            job = GatherMetadataJob(
                settings=JobSettings(
                    metadata_dir=str(runtime_dir),
                    output_dir=str(runtime_dir),
                    subject_id="825546",
                    data_description_settings=DataDescriptionSettings(
                        project_name="Mesoscope test",
                        modalities=[Modality.POPHYS],
                    ),
                )
            )

            with (
                patch.object(job, "copy_original_metadata_files"),
                patch.object(
                    job,
                    "build_data_description",
                    return_value={
                        "name": "multiplane-ophys_825546_2026-02-12_13-04-36",
                        "object_type": "Data description",
                    },
                ),
                patch.object(job, "get_subject", return_value=None),
                patch.object(job, "get_procedures", return_value=None),
                patch.object(job, "get_instrument", return_value=None),
                patch.object(job, "get_processing", return_value=None),
                patch.object(job, "get_quality_control", return_value=None),
                patch.object(job, "get_model", return_value=None),
                patch.object(job, "validate_and_create_metadata", return_value={}),
            ):
                job.run_job()

            self.assertTrue((runtime_dir / "acquisition_mesoscope.json").exists())
            self.assertTrue((runtime_dir / "acquisition.json").exists())
            with open(runtime_dir / "acquisition.json", "r", encoding="utf-8") as input_stream:
                acquisition = Acquisition.model_validate(json.load(input_stream))
            self.assertEqual(acquisition.data_streams[0].configurations[0].device_name, "MESO.2")
            self.assertIn("Behavior camera assembly", acquisition.data_streams[0].active_devices)

    def test_parse_datetime_helper(self) -> None:
        """Datetime helper should preserve aware values and reject bad input."""
        aware_value = self.mapper._parse_datetime("2026-02-12T13:04:36.555247-08:00", "field")
        self.assertEqual(aware_value.isoformat(), "2026-02-12T13:04:36.555247-08:00")

        from datetime import datetime

        now = datetime(2026, 2, 12, 13, 4, 36, tzinfo=ZoneInfo("UTC"))
        self.assertIs(self.mapper._parse_datetime(now, "field"), now)

        with self.assertRaises(ValueError):
            self.mapper._parse_datetime(None, "field")
        with self.assertRaises(TypeError):
            self.mapper._parse_datetime(5, "field")

    def test_parse_float_missing_raises(self) -> None:
        """Numeric helper should require values."""
        with self.assertRaises(ValueError):
            self.mapper._parse_float("", "field")

    def test_camera_name_helpers(self) -> None:
        """Camera helpers should normalize fallback and unlabeled inputs."""
        self.assertEqual(
            self.mapper._extract_camera_label("1770928800_Eye_20260212T130408", {"RecordingReport": {}}),
            "Eye",
        )
        self.assertIsNone(self.mapper._extract_camera_label("unknown_key", {"RecordingReport": {}}))

        session_metadata = {
            "platform": {},
            "camera_a": {"RecordingReport": {"CameraLabel": "Face Camera"}},
            "camera_b": {"RecordingReport": {}},
        }
        self.assertEqual(
            self.mapper._collect_camera_assembly_names(session_metadata),
            ["Face camera assembly"],
        )

        with self.assertRaises(ValueError):
            self.mapper._normalize_camera_assembly_name("   ")

    def test_subject_and_ethics_helpers(self) -> None:
        """Subject and ethics helpers should validate mismatches and empty values."""
        with self.assertRaises(ValueError):
            self.mapper._resolve_subject_id({"subject_id": "1"}, {"subject_id": "2"})
        with self.assertRaises(ValueError):
            self.mapper._resolve_subject_id({}, {})

        self.assertIsNone(self.mapper._resolve_ethics_review_id({}))
        self.assertIsNone(self.mapper._resolve_ethics_review_id({"iacuc_protocol": [""]}))
        self.assertEqual(
            self.mapper._resolve_ethics_review_id({"iacuc_protocol": ["2115", "2414"]}),
            ["2115", "2414"],
        )

    def test_targeted_structure_helper(self) -> None:
        """Targeted structure helper should cover supported and invalid values."""
        structure_dict = {"acronym": "VISp"}
        self.assertEqual(self.mapper._get_targeted_structure(structure_dict), structure_dict)
        self.assertEqual(self.mapper._get_targeted_structure("385")["acronym"], "VISp")

        with self.assertRaises(ValueError):
            self.mapper._get_targeted_structure("")
        with self.assertRaises(ValueError):
            self.mapper._get_targeted_structure("mystery")
        with self.assertRaises(ValueError):
            self.mapper._get_targeted_structure(999)
        with self.assertRaises(TypeError):
            self.mapper._get_targeted_structure(3.14)

    def test_fov_reference_and_dimensions_helpers(self) -> None:
        """Coordinate and dimension helpers should validate required inputs."""
        with self.assertRaises(ValueError):
            self.mapper._get_fov_translation_mm({"fov_reference": "Lambda"})
        with self.assertRaises(ValueError):
            self.mapper._get_image_dimensions([])

    def test_single_plane_group_maps_plane_without_notes(self) -> None:
        """Single-plane groups should emit Plane and allow empty stream notes."""
        metadata = copy.deepcopy(self.fixture)
        metadata["job_settings"].pop("magnification")
        plane = metadata["session_metadata"]["platform"]["imaging_plane_groups"][0]["imaging_planes"][0]
        plane.pop("scanimage_roi_index")
        metadata["session_metadata"]["platform"]["imaging_plane_groups"][0]["imaging_planes"] = [plane]
        metadata["session_metadata"]["platform"]["imaging_plane_groups"][0]["scanimage_power_percent"] = 42
        metadata["session_metadata"]["platform"]["imaging_plane_groups"][0].pop("scanimage_split_percent")

        acquisition = self.mapper.transform(metadata)
        image_plane = acquisition.data_streams[0].configurations[0].images[0].planes[0]

        self.assertEqual(type(image_plane).__name__, "Plane")
        self.assertIsNone(acquisition.data_streams[0].notes)

    def test_empty_plane_groups_raise(self) -> None:
        """Mapper should reject empty plane group lists."""
        metadata = copy.deepcopy(self.fixture)
        metadata["session_metadata"]["platform"]["imaging_plane_groups"] = []

        with self.assertRaises(ValueError) as context:
            self.mapper.transform(metadata)

        self.assertIn("must contain at least one group", str(context.exception))

    def test_missing_power_raises(self) -> None:
        """Mapper should reject missing percent power."""
        metadata = copy.deepcopy(self.fixture)
        group = metadata["session_metadata"]["platform"]["imaging_plane_groups"][0]
        group.pop("scanimage_power_percent")

        with self.assertRaises(ValueError) as context:
            self.mapper.transform(metadata)

        self.assertIn("scanimage_power_percent", str(context.exception))

    def test_performance_metrics_helper(self) -> None:
        """Performance metrics helper should infer reward units when needed."""
        metrics = self.mapper._build_performance_metrics(
            {
                "reward_consumed_during_epoch": 1.5,
                "reward_consumed_unit": None,
                "trials_total": 10,
                "output_parameters": {"task_parameters": {"stage_in_use": "stage-a"}},
            }
        )
        self.assertEqual(metrics.reward_consumed_unit, "milliliter")
        self.assertEqual(metrics.trials_total, 10)

    def test_code_helper_variants(self) -> None:
        """Code helper should handle multiple parameter shapes and errors."""
        multi_code = self.mapper._build_code(
            {
                "stimulus_parameters": [
                    {"stimulus_name": "A", "value": 1},
                    {"stimulus_name": "A", "value": 2},
                ],
                "script": {"name": "script", "version": "1.0", "url": "", "parameters": {"extra": True}},
                "software": [{"name": "camstim", "version": "1.0"}],
            }
        )
        self.assertEqual(multi_code.parameters.model_dump()["A"]["value"], 1)
        self.assertEqual(multi_code.parameters.model_dump()["A_1"]["value"], 2)
        self.assertEqual(multi_code.parameters.model_dump()["A"]["stimulus_type"], "Visual Stimulation")
        self.assertIsNone(multi_code.parameters.model_dump()["A"]["notes"])
        self.assertTrue(multi_code.parameters.model_dump()["extra"])

        preserved_code = self.mapper._build_code(
            {
                "stimulus_parameters": [
                    {
                        "stimulus_name": "B",
                        "stimulus_parameters": {"contrast": [1.0]},
                        "stimulus_template_name": [],
                        "stimulus_type": "Custom visual",
                        "notes": "keep me",
                    }
                ]
            }
        )
        self.assertEqual(preserved_code.parameters.model_dump()["stimulus_type"], "Custom visual")
        self.assertEqual(preserved_code.parameters.model_dump()["notes"], "keep me")

        direct_code = self.mapper._build_code({"stimulus_parameters": {"value": 1}})
        self.assertEqual(direct_code.name, "Code")
        self.assertEqual(direct_code.parameters.model_dump()["value"], 1)

        self.assertIsNone(self.mapper._build_code({}))
        self.assertEqual(
            self.mapper._build_code(
                {"script": {"name": "script", "version": "1.0", "url": "", "parameters": {"extra": True}}}
            ).parameters.model_dump()["extra"],
            True,
        )

        with self.assertRaises(TypeError):
            self.mapper._build_code({"stimulus_parameters": ["bad"]})
        with self.assertRaises(TypeError):
            self.mapper._build_code({"stimulus_parameters": [{"value": 1}, "bad"]})
        with self.assertRaises(TypeError):
            self.mapper._build_code(
                {
                    "stimulus_parameters": 5,
                    "script": {"name": "script", "version": "1.0", "url": "", "parameters": {"extra": True}},
                }
            )


if __name__ == "__main__":
    unittest.main()
