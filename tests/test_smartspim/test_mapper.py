"""Unit tests for SmartSPIM mapper"""

import json
import tempfile
import unittest
from pathlib import Path
from datetime import datetime

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import PowerUnit
from aind_metadata_mapper.smartspim.mapper import SmartspimMapper
from aind_metadata_mapper.base import MapperJobSettings


class TestSmartspimMapper(unittest.TestCase):
    """Test cases for SmartspimMapper"""

    def setUp(self):
        """Set up test fixtures"""
        self.mapper = SmartspimMapper()
        test_data_path = Path(__file__).parent.parent / "resources" / "smartspim" / "smartspim.json"
        with open(test_data_path, "r") as f:
            self.test_metadata = json.load(f)

    def test_transform_complete_workflow(self):
        """Test complete transform workflow from raw metadata to Acquisition"""
        result = self.mapper.transform(self.test_metadata)

        self.assertIsInstance(result, Acquisition)
        self.assertEqual(result.subject_id, "762444")
        self.assertEqual(result.specimen_id, "762444-BRN00000292")
        self.assertEqual(result.instrument_id, "440_SmartSPIM2_20240327")
        self.assertEqual(result.acquisition_type, "SmartSPIM")
        self.assertEqual(result.protocol_id, ["https://dx.doi.org/10.17504/protocols.io.3byl4jo1rlo5/v1"])
        self.assertEqual(result.experimenters, ["EllaHilton-VanOsdall"])
        self.assertEqual(result.stimulus_epochs, [])

        self.assertIsInstance(result.acquisition_start_time, datetime)
        self.assertIsInstance(result.acquisition_end_time, datetime)
        self.assertLess(result.acquisition_start_time, result.acquisition_end_time)

    def test_transform_data_streams_complete(self):
        """Test data streams including configurations and devices"""
        result = self.mapper.transform(self.test_metadata)

        self.assertEqual(len(result.data_streams), 1)
        data_stream = result.data_streams[0]

        self.assertEqual(data_stream.modalities, [Modality.SPIM])
        self.assertEqual(data_stream.stream_start_time, result.acquisition_start_time)
        self.assertEqual(data_stream.stream_end_time, result.acquisition_end_time)
        self.assertGreater(len(data_stream.configurations), 0)
        self.assertGreater(len(data_stream.active_devices), 0)

        imaging_config = None
        for config in data_stream.configurations:
            if hasattr(config, "channels"):
                imaging_config = config
                break

        self.assertIsNotNone(imaging_config)
        self.assertEqual(len(imaging_config.channels), 3)

        for channel in imaging_config.channels:
            self.assertIsNotNone(channel.channel_name)
            self.assertIsNotNone(channel.detector)
            if channel.light_sources:
                for light_source in channel.light_sources:
                    self.assertIsNotNone(light_source.wavelength)

    def test_transform_chamber_config_with_immersion(self):
        """Test chamber config is created with immersion media"""
        result = self.mapper.transform(self.test_metadata)

        data_stream = result.data_streams[0]
        chamber_config = None
        for config in data_stream.configurations:
            if hasattr(config, "chamber_immersion"):
                chamber_config = config
                break

        self.assertIsNotNone(chamber_config)
        self.assertIsNotNone(chamber_config.chamber_immersion)
        self.assertIsNotNone(chamber_config.sample_immersion)

    def test_transform_coordinate_system(self):
        """Test coordinate system is properly built"""
        result = self.mapper.transform(self.test_metadata)

        self.assertIsNotNone(result.coordinate_system)
        self.assertEqual(result.coordinate_system.name, "SPIM_RPI")
        self.assertEqual(len(result.coordinate_system.axes), 3)

    def test_transform_images_from_tiles(self):
        """Test ImageSPIM objects are created from tile configuration"""
        result = self.mapper.transform(self.test_metadata)

        data_stream = result.data_streams[0]
        imaging_config = None
        for config in data_stream.configurations:
            if hasattr(config, "images"):
                imaging_config = config
                break

        self.assertIsNotNone(imaging_config)
        self.assertGreater(len(imaging_config.images), 0)

    def test_run_job_processes_metadata(self):
        """Test run_job method processes metadata correctly"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            input_file = tmpdir_path / "input.json"
            output_file = tmpdir_path / "output_acquisition.json"

            with open(input_file, "w") as f:
                json.dump(self.test_metadata, f)

            job_settings = MapperJobSettings(input_filepath=str(input_file), output_filepath=str(output_file))

            self.mapper.run_job(job_settings)

    def test_specimen_id_construction(self):
        """Test specimen_id construction with different input scenarios"""
        test_data_copy = self.test_metadata.copy()

        test_data_copy["slims_metadata"]["subject_id"] = "999999"
        test_data_copy["slims_metadata"]["specimen_id"] = "SPEC123"
        result = self.mapper.transform(test_data_copy)
        self.assertEqual(result.specimen_id, "999999-SPEC123")

        test_data_copy["slims_metadata"]["subject_id"] = "762444"
        test_data_copy["slims_metadata"]["specimen_id"] = "762444-BRN00000292"
        result = self.mapper.transform(test_data_copy)
        self.assertEqual(result.specimen_id, "762444-BRN00000292")

    def test_experimenter_handling(self):
        """Test experimenter field with fallback logic"""
        result = self.mapper.transform(self.test_metadata)
        self.assertEqual(result.experimenters, ["EllaHilton-VanOsdall"])

        test_data_copy = self.test_metadata.copy()
        test_data_copy["slims_metadata"]["experimenter_name"] = "Custom Experimenter"
        result = self.mapper.transform(test_data_copy)
        self.assertEqual(result.experimenters, ["Custom Experimenter"])

    def test_session_times_with_inverted_timestamps(self):
        """Test session time handling when start/end are inverted"""
        test_data_copy = self.test_metadata.copy()
        test_data_copy["file_metadata"]["session_start_time"] = "2025-07-17T01:48:42"
        test_data_copy["file_metadata"]["session_end_time"] = "2025-07-16T20:47:57"

        result = self.mapper.transform(test_data_copy)
        self.assertLess(result.acquisition_start_time, result.acquisition_end_time)

    def test_wavelength_extraction_formats(self):
        """Test wavelength extraction from standard format"""
        self.assertEqual(self.mapper._extract_wavelength_from_channel("Laser = 488; Emission Filter = 525/45"), 488)
        self.assertEqual(self.mapper._extract_wavelength_from_channel("Laser = 561; Emission Filter = 600"), 561)
        self.assertEqual(self.mapper._extract_wavelength_from_channel("Laser = 639; Emission Filter = 680"), 639)

    def test_power_unit_parsing(self):
        """Test power unit parsing for all supported formats"""
        from aind_data_schema_models.units import PowerUnit

        self.assertEqual(self.mapper._parse_power_unit("milliwatt"), PowerUnit.MW)
        self.assertEqual(self.mapper._parse_power_unit("mw"), PowerUnit.MW)
        self.assertEqual(self.mapper._parse_power_unit("microwatt"), PowerUnit.UW)
        self.assertEqual(self.mapper._parse_power_unit("uw"), PowerUnit.UW)
        self.assertEqual(self.mapper._parse_power_unit("percent"), PowerUnit.PERCENT)
        self.assertEqual(self.mapper._parse_power_unit("invalid"), PowerUnit.PERCENT)

    def test_immersion_medium_mapping(self):
        """Test immersion medium name to enum conversion"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("Cargille Oil 1.5200"), ImmersionMedium.OIL)
        self.assertEqual(self.mapper._map_immersion_medium("cargille oil"), ImmersionMedium.OIL)
        self.assertEqual(self.mapper._map_immersion_medium("EasyIndex"), ImmersionMedium.EASYINDEX)
        self.assertEqual(self.mapper._map_immersion_medium("water"), ImmersionMedium.WATER)
        self.assertEqual(self.mapper._map_immersion_medium("Unknown Medium"), ImmersionMedium.OTHER)
        self.assertEqual(self.mapper._map_immersion_medium(None), ImmersionMedium.AIR)

    def test_emission_filter_with_filter_mapping(self):
        """Test emission filter building from filter_mapping"""
        from aind_metadata_extractor.models.smartspim import SmartspimModel

        test_data_copy = self.test_metadata.copy()
        test_data_copy["file_metadata"]["filter_mapping"] = {"488": 525, "561": 600}
        validated_metadata = SmartspimModel.model_validate(test_data_copy)

        channel_name = "Laser = 488; Emission Filter = 525/45"
        filters, wl = self.mapper._build_emission_filters(channel_name, 488, validated_metadata)
        self.assertEqual(wl, 525)

    def test_light_sources_with_tile_powers(self):
        """Test light source building with left/right tile powers"""
        light_sources = self.mapper._build_light_sources(
            488, {"488": {"power_unit": "milliwatt"}}, {488: {"left": 50.0, "right": 45.0}}
        )
        self.assertGreater(len(light_sources), 0)
        self.assertEqual(light_sources[0].power, 50.0)

    def test_light_sources_with_channel_config_power(self):
        """Test light source building from channel config power fields"""
        light_sources = self.mapper._build_light_sources(
            561, {"561": {"power_left": 100.0, "power_right": 95.0, "power_unit": "mw"}}, {}
        )
        self.assertEqual(len(light_sources), 2)
        self.assertEqual(light_sources[0].power, 100.0)
        self.assertEqual(light_sources[1].power, 95.0)

    def test_light_sources_with_single_power_only(self):
        """Test light source building with only left power"""
        light_sources = self.mapper._build_light_sources(639, {"639": {"power_left": 75.0, "power_unit": "uw"}}, {})
        self.assertEqual(len(light_sources), 1)
        self.assertEqual(light_sources[0].power, 75.0)
        self.assertEqual(light_sources[0].power_unit, PowerUnit.UW)

    def test_light_sources_with_right_power_only(self):
        """Test light source building with only right power"""
        light_sources = self.mapper._build_light_sources(488, {"488": {"power_right": 55.0}}, {})
        self.assertEqual(len(light_sources), 1)
        self.assertEqual(light_sources[0].power, 55.0)

    def test_immersion_medium_easyindex(self):
        """Test EasyIndex immersion medium mapping"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("EasyIndex"), ImmersionMedium.EASYINDEX)

    def test_immersion_medium_acb(self):
        """Test ACB immersion medium mapping"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("ACB"), ImmersionMedium.ACB)

    def test_immersion_medium_ethyl_cinnamate(self):
        """Test ethyl cinnamate immersion medium mapping"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("ethyl cinnamate"), ImmersionMedium.ECI)

    def test_immersion_medium_dih2o(self):
        """Test DIH2O immersion medium mapping"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("DIH2O"), ImmersionMedium.WATER)

    def test_immersion_medium_0_05x_ssc(self):
        """Test 0.05x SSC immersion medium mapping"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("0.05X SSC"), ImmersionMedium.WATER)

    def test_process_session_times_with_inverted_strings(self):
        """Test session time processing when provided as strings"""
        start, end = self.mapper._process_session_times("2025-01-01T10:00:00", "2025-01-01T15:00:00")
        self.assertLess(start, end)

    def test_process_session_times_with_none_start(self):
        """Test session time processing with None start time"""
        start, end = self.mapper._process_session_times(None, "2025-01-01T15:00:00")
        self.assertIsNotNone(start)

    def test_build_coordinate_system_missing_directions(self):
        """Test coordinate system returns None when directions are missing"""
        from aind_metadata_extractor.models.smartspim import SmartspimModel

        test_data_copy = self.test_metadata.copy()
        test_data_copy["slims_metadata"]["x_direction"] = None
        validated_metadata = SmartspimModel.model_validate(test_data_copy)
        result = self.mapper._build_coordinate_system(validated_metadata)
        self.assertIsNone(result)

    def test_immersion_medium_partial_match(self):
        """Test immersion medium with partial string match"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("Oil Cargille 1.52"), ImmersionMedium.OIL)

    def test_immersion_medium_unknown(self):
        """Test immersion medium with completely unknown value"""
        from aind_data_schema_models.devices import ImmersionMedium

        self.assertEqual(self.mapper._map_immersion_medium("xyz_completely_unknown_abc"), ImmersionMedium.OTHER)


if __name__ == "__main__":
    unittest.main()
