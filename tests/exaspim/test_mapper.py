"""Tests for aind_metadata_mapper.exaspim.mapper."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aind_metadata_mapper.base import MapperJobSettings
from aind_metadata_mapper.exaspim.constants import (
    DEPRECATED_MOTORIZED_FIELDS,
)
from aind_metadata_mapper.exaspim.mapper import (
    ExaSPIMMapper,
    _contains_exaspim_keyword,
    _load_json,
    _needs_upgrade,
    _resolve_instrument_from_yaml,
    _to_json_dict,
    _write_json,
    sanitize_instrument,
    upgrade_acquisition_only,
    upgrade_with_instrument,
)

# ---------------------------------------------------------------------------
# Inline v1 fixture data (no external JSON files)
# ---------------------------------------------------------------------------

_MFR_VIEWORKS = {
    "name": "Vieworks",
    "abbreviation": None,
    "registry": None,
    "registry_identifier": None,
}
_MFR_OXXIUS = {
    "name": "Oxxius",
    "abbreviation": None,
    "registry": None,
    "registry_identifier": None,
}
_MFR_ASI = {
    "name": "Applied Scientific Instrumentation",
    "abbreviation": "ASI",
    "registry": None,
    "registry_identifier": None,
}
_MFR_NI = {
    "name": "National Instruments",
    "abbreviation": None,
    "registry": {
        "name": "Research Organization Registry",
        "abbreviation": "ROR",
    },
    "registry_identifier": "026exqw73",
}

_V1_ACQUISITION = {
    "describedBy": (
        "https://raw.githubusercontent.com/AllenNeuralDynamics/"
        "aind-data-schema/main/src/aind_data_schema/core/acquisition.py"
    ),
    "schema_version": "1.0.4",
    "protocol_id": [],
    "experimenter_full_name": ["adam glaser"],
    "specimen_id": "822178-1x",
    "subject_id": "822178-1x",
    "instrument_id": "440_exaSPIM1-20231004",
    "calibrations": [],
    "maintenance": [],
    "session_start_time": "2026-04-03T15:46:33.698599-07:00",
    "session_end_time": "2026-04-03T16:19:57.552352-07:00",
    "session_type": None,
    "tiles": [
        {
            "coordinate_transformations": [
                {"type": "scale", "scale": ["15.04", "15.04", "20.0"]},
                {
                    "type": "translation",
                    "translation": ["-41.5744", "13.1057", "-23.799"],
                },
            ],
            "file_name": "tile_000000_ch_561.ims",
            "channel": {
                "channel_name": "561",
                "light_source_name": "561 nm",
                "filter_names": [],
                "detector_name": "vnp-604mx",
                "additional_device_names": [],
                "excitation_wavelength": 561,
                "excitation_wavelength_unit": "nanometer",
                "excitation_power": 220.0,
                "excitation_power_unit": "milliwatt",
                "filter_wheel_index": 0,
                "dilation": None,
                "dilation_unit": "pixel",
                "description": None,
            },
            "notes": None,
            "imaging_angle": 0,
            "imaging_angle_unit": "degrees",
            "acquisition_start_time": None,
            "acquisition_end_time": None,
        },
    ],
    "axes": [
        {
            "name": "X",
            "direction": "Inferior_to_superior",
            "dimension": 2,
            "unit": "micrometer",
        },
        {
            "name": "Y",
            "direction": "Anterior_to_posterior",
            "dimension": 1,
            "unit": "micrometer",
        },
        {
            "name": "Z",
            "direction": "Left_to_right",
            "dimension": 0,
            "unit": "micrometer",
        },
    ],
    "chamber_immersion": {"medium": "other", "refractive_index": "1.33"},
    "sample_immersion": None,
    "active_objectives": None,
    "local_storage_directory": ("D:\\exaSPIM_822178-1x_2026-04-03_15-46-33"),
    "external_storage_directory": ("Z:\\stage\\exaSPIM\\exaSPIM_822178-1x_2026-04-03_15-46-33"),
    "processing_steps": [],
    "software": [],
    "notes": None,
}

_V1_INSTRUMENT = {
    "describedBy": (
        "https://raw.githubusercontent.com/AllenNeuralDynamics/"
        "aind-data-schema/main/src/aind_data_schema/core/instrument.py"
    ),
    "schema_version": "1.0.4",
    "instrument_id": "440_exaSPIM1-20231004",
    "modification_date": "2023-10-04",
    "instrument_type": "exaSPIM",
    "manufacturer": {
        "name": "Custom",
        "abbreviation": None,
        "registry": None,
        "registry_identifier": None,
    },
    "temperature_control": False,
    "humidity_control": False,
    "optical_tables": [
        {
            "device_type": "Optical table",
            "name": "Table",
            "serial_number": None,
            "manufacturer": {
                "name": "MKS Newport",
                "abbreviation": None,
                "registry": {
                    "name": "Research Organization Registry",
                    "abbreviation": "ROR",
                },
                "registry_identifier": "00k17f049",
            },
            "model": "VIS3648-PG2-325A",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "length": "36",
            "width": "48",
            "table_size_unit": "inch",
            "vibration_control": True,
        }
    ],
    "enclosure": None,
    # objectives[0] has a recognised manufacturer (Vieworks);
    # objectives[1] has an unrecognised one (PhotonGear).
    "objectives": [
        {
            "device_type": "Objective",
            "name": "Detection Objective",
            "serial_number": None,
            "manufacturer": _MFR_VIEWORKS,
            "model": "JM_DIAMOND 5.0X/1.3",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": ("manufacturer collaboration between " "Schneider-Kreuznach and Vieworks"),
            "numerical_aperture": "0.305",
            "magnification": "5",
            "immersion": "air",
            "objective_type": None,
        },
        {
            "device_type": "Objective",
            "name": "Excitation Objective",
            "serial_number": None,
            "manufacturer": {
                "name": "PhotonGear",
                "abbreviation": None,
                "registry": None,
                "registry_identifier": None,
            },
            "model": "Custom",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "numerical_aperture": "0.4",
            "magnification": "10",
            "immersion": "air",
            "objective_type": None,
        },
    ],
    "detectors": [
        {
            "device_type": "Detector",
            "name": "Camera 1",
            "serial_number": "MB151BAY001",
            "manufacturer": _MFR_VIEWORKS,
            "model": "VNP-604MX",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "detector_type": "Camera",
            "data_interface": "Coax",
            "cooling": "Air",
            "computer_name": None,
            "frame_rate": None,
            "frame_rate_unit": "hertz",
            "immersion": None,
            "chroma": None,
            "sensor_width": None,
            "sensor_height": None,
            "size_unit": "pixel",
            "sensor_format": None,
            "sensor_format_unit": None,
            "bit_depth": None,
            "bin_mode": "None",
            "bin_width": None,
            "bin_height": None,
            "bin_unit": "pixel",
            "gain": None,
            "crop_offset_x": None,
            "crop_offset_y": None,
            "crop_width": None,
            "crop_height": None,
            "crop_unit": "pixel",
            "recording_software": None,
            "driver": None,
            "driver_version": None,
        }
    ],
    "light_sources": [
        {
            "device_type": "Laser",
            "name": "539251",
            "serial_number": "539251",
            "manufacturer": _MFR_OXXIUS,
            "model": None,
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": "Housed in commercial laser combiner",
            "wavelength": 561,
            "wavelength_unit": "nanometer",
            "maximum_power": "200",
            "power_unit": "milliwatt",
            "coupling": "Single-mode fiber",
            "coupling_efficiency": None,
            "coupling_efficiency_unit": "percent",
            "item_number": None,
        }
    ],
    "lenses": [],
    "fluorescence_filters": [
        {
            "device_type": "Filter",
            "name": "Multiband filter",
            "serial_number": None,
            "manufacturer": {
                "name": "Chroma",
                "abbreviation": None,
                "registry": None,
                "registry_identifier": None,
            },
            "model": "ZET405/488/561/640mv2",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": "Custom made filter",
            "filter_type": "Multiband",
            "diameter": "44.05",
            "width": None,
            "height": None,
            "size_unit": "millimeter",
            "thickness": "1",
            "thickness_unit": "millimeter",
            "filter_wheel_index": 0,
            "cut_off_wavelength": None,
            "cut_on_wavelength": None,
            "center_wavelength": None,
            "wavelength_unit": "nanometer",
            "description": None,
        }
    ],
    # motorized_stages carry deprecated fields and invalid travel_unit.
    "motorized_stages": [
        {
            "device_type": "Motorized stage",
            "name": "detection-motor",
            "serial_number": None,
            "manufacturer": _MFR_ASI,
            "model": "MS-8000",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "travel": "1000",
            "travel_unit": "millimeter",
            "firmware": None,
            "stage_axis_direction": "Detection axis",
            "stage_axis_name": "X",
        },
        {
            "device_type": "Motorized stage",
            "name": "illumination-motor",
            "serial_number": None,
            "manufacturer": _MFR_ASI,
            "model": "LS-100",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "travel": "100",
            "travel_unit": "degree",
            "firmware": None,
            "stage_axis_direction": "Illumination axis",
            "stage_axis_name": "Z",
        },
    ],
    # scanning_stages carry free-form axis directions that need mapping.
    "scanning_stages": [
        {
            "device_type": "Motorized stage",
            "name": "stage-x",
            "serial_number": None,
            "manufacturer": _MFR_ASI,
            "model": "MS-8000",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "travel": "1000",
            "travel_unit": "millimeter",
            "firmware": None,
            "stage_axis_direction": "Detection Z axis",
            "stage_axis_name": "X",
        },
        {
            "device_type": "Motorized stage",
            "name": "stage-y",
            "serial_number": None,
            "manufacturer": _MFR_ASI,
            "model": "MS-8000",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "travel": "1000",
            "travel_unit": "millimeter",
            "firmware": None,
            "stage_axis_direction": "Perpendicular to detection",
            "stage_axis_name": "Y",
        },
        {
            "device_type": "Motorized stage",
            "name": "stage-z",
            "serial_number": None,
            "manufacturer": _MFR_ASI,
            "model": "LS-100",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "travel": "100",
            "travel_unit": "millimeter",
            "firmware": None,
            "stage_axis_direction": "Illumination axis",
            "stage_axis_name": "Z",
        },
    ],
    "additional_devices": [
        {
            "device_type": "Additional imaging device",
            "name": "TL-1",
            "serial_number": "01",
            "manufacturer": {
                "name": "Optotune",
                "abbreviation": None,
                "registry": None,
                "registry_identifier": None,
            },
            "model": "EL-16-40-TC-VIS-20D-C",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "imaging_device_type": "Tunable lens",
        }
    ],
    "calibration_date": None,
    "calibration_data": None,
    "com_ports": [
        {"hardware_name": "Laser Launch", "com_port": "COM2"},
        {"hardware_name": "ASI Tiger", "com_port": "COM5"},
    ],
    "daqs": [
        {
            "device_type": "DAQ Device",
            "name": "Dev2",
            "serial_number": None,
            "manufacturer": _MFR_NI,
            "model": "PCIe-6738",
            "path_to_cad": None,
            "port_index": None,
            "additional_settings": {},
            "notes": None,
            "data_interface": "USB",
            "computer_name": "Dev2",
            "channels": [
                {
                    "channel_name": "5",
                    "device_name": "539251",
                    "channel_type": "Analog Output",
                    "port": None,
                    "channel_index": None,
                    "sample_rate": "10000",
                    "sample_rate_unit": "hertz",
                    "event_based_sampling": None,
                }
            ],
            "firmware_version": None,
            "hardware_version": None,
        }
    ],
    "notes": None,
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def v1_acq() -> dict:
    """Return a deep copy of the inline v1 acquisition data."""
    return json.loads(json.dumps(_V1_ACQUISITION))


@pytest.fixture
def v1_inst() -> dict:
    """Return a deep copy of the inline v1 instrument data."""
    return json.loads(json.dumps(_V1_INSTRUMENT))


@pytest.fixture
def v2_acq() -> dict:
    """Minimal already-v2 acquisition dict."""
    return {"schema_version": "2.5.0", "data_streams": []}


@pytest.fixture
def tmp_metadata_dir(v1_acq, v1_inst):
    """Create a temp dir with v1 acquisition and instrument files."""
    with tempfile.TemporaryDirectory() as td:
        acq_path = Path(td) / "acquisition.json"
        inst_path = Path(td) / "instrument.json"
        with open(acq_path, "w") as f:
            json.dump(v1_acq, f)
        with open(inst_path, "w") as f:
            json.dump(v1_inst, f)
        yield Path(td)


def _make_job_settings(metadata_dir: Path) -> MapperJobSettings:
    """Helper to create MapperJobSettings for a metadata directory."""
    return MapperJobSettings(
        input_filepath=metadata_dir / "acquisition.json",
        output_directory=metadata_dir,
        output_filename_suffix="exaspim",
    )


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    """Tests for module-level helper functions."""

    def test_load_json_valid(self, tmp_path):
        """Test loading a valid JSON file."""
        p = tmp_path / "test.json"
        p.write_text('{"key": "value"}')
        result = _load_json(p)
        assert result == {"key": "value"}

    def test_load_json_missing(self, tmp_path):
        """Test loading a missing file returns None."""
        result = _load_json(tmp_path / "nonexistent.json")
        assert result is None

    def test_load_json_empty(self, tmp_path):
        """Test loading an empty file returns None."""
        p = tmp_path / "empty.json"
        p.write_text("{}")
        result = _load_json(p)
        assert result is None

    def test_load_json_invalid(self, tmp_path):
        """Test loading invalid JSON returns None."""
        p = tmp_path / "bad.json"
        p.write_text("not json at all")
        result = _load_json(p)
        assert result is None

    def test_write_json(self, tmp_path):
        """Test writing a JSON file."""
        p = tmp_path / "out.json"
        _write_json(p, {"hello": "world"})
        with open(p) as f:
            data = json.load(f)
        assert data == {"hello": "world"}

    def test_needs_upgrade_v1(self):
        """v1 data needs upgrade."""
        assert _needs_upgrade({"schema_version": "1.0.4"}) is True

    def test_needs_upgrade_v0(self):
        """v0 data needs upgrade."""
        assert _needs_upgrade({"schema_version": "0.9.1"}) is True

    def test_needs_upgrade_v2(self):
        """v2 data does not need upgrade."""
        assert _needs_upgrade({"schema_version": "2.0.0"}) is False

    def test_needs_upgrade_v2_plus(self):
        """v2.5+ data does not need upgrade."""
        assert _needs_upgrade({"schema_version": "2.5.1"}) is False

    def test_needs_upgrade_missing_version(self):
        """Missing schema_version defaults to 0.0.0 → needs upgrade."""
        assert _needs_upgrade({}) is True

    def test_to_json_dict_none(self):
        """None returns None."""
        assert _to_json_dict(None) is None

    def test_to_json_dict_with_model_dump(self):
        """Objects with model_dump are serialised."""
        mock = MagicMock()
        mock.model_dump.return_value = {"a": 1}
        result = _to_json_dict(mock)
        assert result == {"a": 1}
        mock.model_dump.assert_called_once_with(mode="json", exclude_none=True)

    def test_contains_exaspim_keyword(self):
        """Various keyword checks."""
        assert _contains_exaspim_keyword("exaSPIM-beta-01") is True
        assert _contains_exaspim_keyword("EXASPIM") is True
        assert _contains_exaspim_keyword("ExaSPIM") is True
        assert _contains_exaspim_keyword("mesoscope") is False
        assert _contains_exaspim_keyword("") is False


# ---------------------------------------------------------------------------
# Test detection
# ---------------------------------------------------------------------------


class TestExaSPIMDetection:
    """Tests for ExaSPIMMapper.detect()."""

    def test_detects_instrument_id(self, tmp_path):
        """Detects via instrument_id field."""
        inst = {"instrument_id": "exaSPIM-beta-01", "instrument_type": ""}
        (tmp_path / "instrument.json").write_text(json.dumps(inst))
        assert ExaSPIMMapper.detect(tmp_path) is True

    def test_detects_instrument_type(self, tmp_path):
        """Detects via instrument_type field."""
        inst = {"instrument_id": "something-else", "instrument_type": "exaSPIM"}
        (tmp_path / "instrument.json").write_text(json.dumps(inst))
        assert ExaSPIMMapper.detect(tmp_path) is True

    def test_case_insensitive(self, tmp_path):
        """Detection is case-insensitive."""
        inst = {"instrument_id": "EXASPIM-test", "instrument_type": ""}
        (tmp_path / "instrument.json").write_text(json.dumps(inst))
        assert ExaSPIMMapper.detect(tmp_path) is True

    def test_non_exaspim_instrument(self, tmp_path):
        """Non-exaSPIM instrument is not detected."""
        inst = {"instrument_id": "mesoscope-01", "instrument_type": "mesoscope"}
        (tmp_path / "instrument.json").write_text(json.dumps(inst))
        assert ExaSPIMMapper.detect(tmp_path) is False

    def test_no_instrument_file(self, tmp_path):
        """No instrument.json and no yaml → not detected."""
        assert ExaSPIMMapper.detect(tmp_path) is False

    def test_yaml_fallback(self, tmp_path):
        """Detects via instrument_config.yaml fallback."""
        yaml_dir = tmp_path / "derivatives"
        yaml_dir.mkdir()
        (yaml_dir / "instrument_config.yaml").write_text("instrument_type: exaSPIM\nrig_id: exaSPIM-beta-01\n")
        assert ExaSPIMMapper.detect(tmp_path) is True

    def test_yaml_fallback_no_match(self, tmp_path):
        """YAML fallback with non-matching content → not detected."""
        yaml_dir = tmp_path / "derivatives"
        yaml_dir.mkdir()
        (yaml_dir / "instrument_config.yaml").write_text("instrument_type: mesoscope\nrig_id: meso-01\n")
        assert ExaSPIMMapper.detect(tmp_path) is False

    def test_empty_instrument_json(self, tmp_path):
        """Empty instrument.json → not detected (no crash)."""
        (tmp_path / "instrument.json").write_text("{}")
        assert ExaSPIMMapper.detect(tmp_path) is False

    def test_invalid_yaml_no_crash(self, tmp_path):
        """Invalid YAML doesn't crash detection."""
        yaml_dir = tmp_path / "derivatives"
        yaml_dir.mkdir()
        (yaml_dir / "instrument_config.yaml").write_text(":\n  - [\ninvalid yaml content")
        assert ExaSPIMMapper.detect(tmp_path) is False

    def test_yaml_fallback_non_dict_content(self, tmp_path):
        """YAML with non-dict content (e.g. a list) is searched as string."""
        yaml_dir = tmp_path / "derivatives"
        yaml_dir.mkdir()
        (yaml_dir / "instrument_config.yaml").write_text("- exaSPIM-beta-01\n- some_other_thing\n")
        assert ExaSPIMMapper.detect(tmp_path) is True


# ---------------------------------------------------------------------------
# Test sanitize_instrument
# ---------------------------------------------------------------------------


class TestSanitizeInstrument:
    """Tests for the 5-step instrument sanitisation."""

    def test_unrecognised_manufacturer_replaced(self, v1_inst):
        """Unrecognised manufacturer 'PhotonGear' → 'Other'."""
        result = sanitize_instrument(v1_inst)
        excitation_obj = result["objectives"][1]
        assert excitation_obj["manufacturer"]["name"] == "Other"
        assert "PhotonGear" in excitation_obj["notes"]
        assert "(v1v2 pre-process)" in excitation_obj["notes"]

    def test_recognised_manufacturer_unchanged(self, v1_inst):
        """Recognised manufacturer 'Vieworks' remains unchanged."""
        result = sanitize_instrument(v1_inst)
        assert result["objectives"][0]["manufacturer"]["name"] == "Vieworks"

    def test_missing_magnification_defaulted(self):
        """Missing magnification on objective → default 1.0."""
        inst = {
            "objectives": [
                {
                    "name": "No-mag objective",
                    "manufacturer": {"name": "Other"},
                }
            ],
        }
        result = sanitize_instrument(inst)
        assert result["objectives"][0]["magnification"] == 1.0
        assert "magnification was missing" in result["objectives"][0]["notes"]

    def test_magnification_present_unchanged(self, v1_inst):
        """Objective with magnification keeps original value."""
        result = sanitize_instrument(v1_inst)
        # Detection objective has magnification "5"
        assert result["objectives"][0]["magnification"] == "5"

    def test_multiband_filter_wavelength_parsed(self, v1_inst):
        """Multiband filter center_wavelength parsed from model name."""
        result = sanitize_instrument(v1_inst)
        mb_filter = result["fluorescence_filters"][0]
        assert mb_filter["center_wavelength"] == [405, 488, 561, 640]

    def test_multiband_filter_with_existing_wavelength(self):
        """Multiband filter with existing center_wavelength is unchanged."""
        inst = {
            "fluorescence_filters": [
                {
                    "name": "F1",
                    "filter_type": "Multiband",
                    "model": "ZET405/488m",
                    "center_wavelength": [405],
                }
            ],
        }
        result = sanitize_instrument(inst)
        assert result["fluorescence_filters"][0]["center_wavelength"] == [405]

    def test_multiband_filter_unparseable_model(self):
        """Multiband filter with unparseable model gets no wavelength."""
        inst = {
            "fluorescence_filters": [
                {
                    "name": "Mystery",
                    "filter_type": "Multiband",
                    "model": "custom-xyz",
                    "center_wavelength": None,
                }
            ],
        }
        result = sanitize_instrument(inst)
        # center_wavelength remains None (not set to empty list)
        assert result["fluorescence_filters"][0]["center_wavelength"] is None

    def test_deprecated_motorized_fields_stripped(self, v1_inst):
        """Deprecated fields removed from motorized_stages."""
        result = sanitize_instrument(v1_inst)
        for stage in result["motorized_stages"]:
            for field in DEPRECATED_MOTORIZED_FIELDS:
                assert field not in stage
            assert "(v1v2 pre-process)" in stage["notes"]

    def test_invalid_travel_unit_remapped(self, v1_inst):
        """Invalid travel_unit 'degree' → 'millimeter'."""
        result = sanitize_instrument(v1_inst)
        illum_stage = result["motorized_stages"][1]
        assert illum_stage["travel_unit"] == "millimeter"
        assert "original travel_unit was 'degree'" in illum_stage["notes"]

    def test_valid_travel_unit_unchanged(self, v1_inst):
        """Valid travel_unit 'millimeter' is not touched."""
        result = sanitize_instrument(v1_inst)
        detect_stage = result["motorized_stages"][0]
        assert detect_stage["travel_unit"] == "millimeter"

    def test_scanning_stage_axis_direction_normalised(self, v1_inst):
        """Free-form direction mapped to canonical value."""
        result = sanitize_instrument(v1_inst)
        x_stage = result["scanning_stages"][0]
        assert x_stage["stage_axis_direction"] == "Detection axis"
        assert "Detection Z axis" in x_stage["notes"]

    def test_scanning_stage_perpendicular_keyword(self, v1_inst):
        """'Perpendicular to detection' → 'Perpendicular axis'."""
        result = sanitize_instrument(v1_inst)
        rot_stage = result["scanning_stages"][1]
        assert rot_stage["stage_axis_direction"] == "Perpendicular axis"

    def test_scanning_stage_unknown_direction_defaults(self):
        """Unknown direction defaults to 'Detection axis'."""
        inst = {
            "scanning_stages": [
                {
                    "name": "mystery stage",
                    "stage_axis_direction": "some random text",
                }
            ],
        }
        result = sanitize_instrument(inst)
        assert result["scanning_stages"][0]["stage_axis_direction"] == "Detection axis"

    def test_scanning_stage_valid_direction_unchanged(self):
        """Already-valid direction is not modified."""
        inst = {
            "scanning_stages": [
                {
                    "name": "good stage",
                    "stage_axis_direction": "Illumination axis",
                    "stage_axis_name": "Z",
                }
            ],
        }
        result = sanitize_instrument(inst)
        assert result["scanning_stages"][0]["stage_axis_direction"] == "Illumination axis"
        assert result["scanning_stages"][0].get("notes") is None

    def test_does_not_mutate_original(self, v1_inst):
        """Sanitise returns a deep copy; original is unchanged."""
        original_name = v1_inst["objectives"][1]["manufacturer"]["name"]
        sanitize_instrument(v1_inst)
        assert v1_inst["objectives"][1]["manufacturer"]["name"] == original_name


# ---------------------------------------------------------------------------
# Test upgrade functions
# ---------------------------------------------------------------------------


class TestUpgradeWithInstrument:
    """Tests for upgrade_with_instrument."""

    @patch("aind_metadata_mapper.exaspim.mapper.sanitize_instrument")
    @patch("aind_metadata_mapper.exaspim.mapper.Upgrade", create=True)
    def test_calls_sanitize_and_upgrade(self, mock_upgrade_cls, mock_sanitize):
        """Sanitises instrument and calls Upgrade()."""
        mock_sanitize.return_value = {"instrument_id": "sanitized"}

        # Set up the mock Upgrade instance
        mock_instance = MagicMock()
        mock_instance.metadata.acquisition = MagicMock()
        mock_instance.metadata.acquisition.model_dump.return_value = {"schema_version": "2.5.1"}
        mock_instance.metadata.instrument = MagicMock()
        mock_instance.metadata.instrument.model_dump.return_value = {"schema_version": "2.5.1"}
        mock_upgrade_cls.return_value = mock_instance

        # Test directly through the function
        acq_data = {"schema_version": "1.0.4", "tiles": []}
        inst_data = {"instrument_id": "exaSPIM", "schema_version": "0.9.1"}

        # Patch at the point of import inside the function
        with patch(
            "aind_metadata_mapper.exaspim.mapper.sanitize_instrument",
            return_value={"instrument_id": "sanitized"},
        ) as mock_san:
            with patch("aind_metadata_upgrader.upgrade.Upgrade") as mock_upg_cls:
                mock_up = MagicMock()
                mock_up.metadata.acquisition = MagicMock()
                mock_up.metadata.acquisition.model_dump.return_value = {
                    "schema_version": "2.5.1",
                    "data_streams": [],
                }
                mock_up.metadata.instrument = MagicMock()
                mock_up.metadata.instrument.model_dump.return_value = {
                    "schema_version": "2.5.1",
                }
                mock_upg_cls.return_value = mock_up

                result_acq, result_inst = upgrade_with_instrument(acq_data, inst_data)

                mock_san.assert_called_once_with(inst_data)
                mock_upg_cls.assert_called_once()
                call_args = mock_upg_cls.call_args
                assert call_args[0][0] == {
                    "acquisition": acq_data,
                    "instrument": {"instrument_id": "sanitized"},
                }
                assert call_args[1]["skip_metadata_validation"] is True

        assert result_acq == {"schema_version": "2.5.1", "data_streams": []}
        assert result_inst == {"schema_version": "2.5.1"}


class TestUpgradeAcquisitionOnly:
    """Tests for upgrade_acquisition_only."""

    @patch("aind_metadata_upgrader.acquisition.v1v2.AcquisitionV1V2")
    @patch("aind_data_schema.core.acquisition.Acquisition")
    def test_calls_upgrader_with_stub(self, mock_acq_cls, mock_v1v2_cls):
        """Calls AcquisitionV1V2 with empty stub metadata."""
        mock_upgrader = MagicMock()
        mock_upgrader.upgrade.return_value = {
            "schema_version": "2.5.1",
            "data_streams": [],
        }
        mock_v1v2_cls.return_value = mock_upgrader

        mock_model = MagicMock()
        mock_model.model_dump.return_value = {
            "schema_version": "2.5.1",
            "data_streams": [],
        }
        mock_acq_cls.model_construct.return_value = mock_model
        mock_acq_cls.model_fields = {"schema_version": MagicMock(default="2.5.1")}

        acq_data = {"schema_version": "1.0.4", "tiles": [], "axes": []}
        result = upgrade_acquisition_only(acq_data)

        mock_upgrader.upgrade.assert_called_once()
        call_args = mock_upgrader.upgrade.call_args
        # First arg is a copy of acq_data
        assert call_args[0][0] == acq_data
        # Second arg is target version
        assert call_args[0][1] == "2.5.1"
        # metadata kwarg has empty instrument stub
        assert call_args[1]["metadata"] == {
            "instrument": {
                "fluorescence_filters": [],
                "light_sources": [],
            }
        }
        assert result == {"schema_version": "2.5.1", "data_streams": []}


# ---------------------------------------------------------------------------
# Test ExaSPIMMapper.run_job
# ---------------------------------------------------------------------------


class TestExaSPIMMapperRunJob:
    """Tests for ExaSPIMMapper.run_job integration."""

    def test_missing_acquisition_raises(self, tmp_path):
        """Raises FileNotFoundError when acquisition.json is missing."""
        mapper = ExaSPIMMapper()
        with pytest.raises(FileNotFoundError, match="acquisition.json"):
            mapper.run_job(_make_job_settings(tmp_path))

    def test_already_v2_is_noop(self, tmp_path, v2_acq):
        """v2 acquisition.json → no change."""
        acq_path = tmp_path / "acquisition.json"
        acq_path.write_text(json.dumps(v2_acq))
        # Also need instrument for detection (but run_job doesn't require it)
        inst = {
            "instrument_id": "exaSPIM-beta-01",
            "instrument_type": "exaSPIM",
            "schema_version": "2.5.0",
        }
        (tmp_path / "instrument.json").write_text(json.dumps(inst))

        mapper = ExaSPIMMapper()
        mapper.run_job(_make_job_settings(tmp_path))

        # File unchanged
        with open(acq_path) as f:
            result = json.load(f)
        assert result == v2_acq

    @patch("aind_metadata_mapper.exaspim.mapper.upgrade_with_instrument")
    def test_v1_with_instrument_upgrades_both(self, mock_upgrade, tmp_metadata_dir):
        """v1 with instrument → both files upgraded."""
        mock_upgrade.return_value = (
            {"schema_version": "2.5.1", "data_streams": []},
            {"schema_version": "2.5.1", "instrument_id": "exaSPIM-beta-01"},
        )

        mapper = ExaSPIMMapper()
        mapper.run_job(_make_job_settings(tmp_metadata_dir))

        mock_upgrade.assert_called_once()

        # Check files were written
        with open(tmp_metadata_dir / "acquisition.json") as f:
            acq = json.load(f)
        assert acq["schema_version"] == "2.5.1"

        with open(tmp_metadata_dir / "instrument.json") as f:
            inst = json.load(f)
        assert inst["schema_version"] == "2.5.1"

    @patch("aind_metadata_mapper.exaspim.mapper.upgrade_acquisition_only")
    def test_v1_without_instrument_upgrades_acq(self, mock_upgrade, tmp_path, v1_acq):
        """v1 without instrument → only acquisition upgraded."""
        (tmp_path / "acquisition.json").write_text(json.dumps(v1_acq))
        mock_upgrade.return_value = {
            "schema_version": "2.5.1",
            "data_streams": [],
        }

        mapper = ExaSPIMMapper()
        mapper.run_job(_make_job_settings(tmp_path))

        mock_upgrade.assert_called_once()

        with open(tmp_path / "acquisition.json") as f:
            acq = json.load(f)
        assert acq["schema_version"] == "2.5.1"

        # No instrument.json should exist
        assert not (tmp_path / "instrument.json").exists()

    @patch("aind_metadata_mapper.exaspim.mapper.upgrade_with_instrument")
    def test_upgrade_returns_none_raises(self, mock_upgrade, tmp_metadata_dir):
        """RuntimeError when upgrader returns None."""
        mock_upgrade.return_value = (None, None)

        mapper = ExaSPIMMapper()
        with pytest.raises(RuntimeError, match="did not produce"):
            mapper.run_job(_make_job_settings(tmp_metadata_dir))


# ---------------------------------------------------------------------------
# Test real upgrader integration (requires aind-metadata-upgrader)
# ---------------------------------------------------------------------------


class TestRealUpgraderIntegration:
    """Integration tests using the real aind-metadata-upgrader.

    These tests verify that our fixture data actually upgrades
    successfully through the real upgrader pipeline.
    """

    def test_upgrade_with_instrument_real(self, tmp_metadata_dir):
        """Real upgrade of v1 acquisition + instrument."""
        mapper = ExaSPIMMapper()
        mapper.run_job(_make_job_settings(tmp_metadata_dir))

        with open(tmp_metadata_dir / "acquisition.json") as f:
            acq = json.load(f)
        with open(tmp_metadata_dir / "instrument.json") as f:
            inst = json.load(f)

        # Schema version should be >= 2.0.0
        from packaging import version

        assert version.parse(acq["schema_version"]) >= version.parse("2.0.0")
        assert version.parse(inst["schema_version"]) >= version.parse("2.0.0")

        # v2 acquisition uses data_streams instead of tiles
        assert "data_streams" in acq

    def test_upgrade_acquisition_only_real(self, tmp_path, v1_acq):
        """Real upgrade of v1 acquisition without instrument."""
        (tmp_path / "acquisition.json").write_text(json.dumps(v1_acq))

        mapper = ExaSPIMMapper()
        mapper.run_job(_make_job_settings(tmp_path))

        with open(tmp_path / "acquisition.json") as f:
            acq = json.load(f)

        from packaging import version

        assert version.parse(acq["schema_version"]) >= version.parse("2.0.0")
        assert "data_streams" in acq


# ---------------------------------------------------------------------------
# Test _resolve_instrument_from_yaml
# ---------------------------------------------------------------------------


class TestResolveInstrumentFromYaml:
    """Tests for _resolve_instrument_from_yaml helper."""

    def test_resolves_known_id(self, tmp_path):
        """Known instrument id returns the reference JSON dict."""
        deriv = tmp_path / "derivatives"
        deriv.mkdir()
        (deriv / "instrument_config.yaml").write_text("instrument:\n  id: exaspim-01\n  channels: {}\n")
        result = _resolve_instrument_from_yaml(tmp_path)
        assert result is not None
        assert "schema_version" in result
        # beta02 is the expected mapping for exaspim-01
        assert "instrument_id" in result

    def test_resolves_1x_id(self, tmp_path):
        """exaspim-1x maps to 1x_instrument.json."""
        deriv = tmp_path / "derivatives"
        deriv.mkdir()
        (deriv / "instrument_config.yaml").write_text("instrument:\n  id: exaspim-1x\n")
        result = _resolve_instrument_from_yaml(tmp_path)
        assert result is not None
        assert "schema_version" in result

    def test_unknown_id_returns_none(self, tmp_path):
        """Unknown instrument id returns None."""
        deriv = tmp_path / "derivatives"
        deriv.mkdir()
        (deriv / "instrument_config.yaml").write_text("instrument:\n  id: unknown-scope\n")
        result = _resolve_instrument_from_yaml(tmp_path)
        assert result is None

    def test_missing_yaml_returns_none(self, tmp_path):
        """No instrument_config.yaml → None."""
        result = _resolve_instrument_from_yaml(tmp_path)
        assert result is None

    def test_malformed_yaml_returns_none(self, tmp_path):
        """Malformed YAML → None (no crash)."""
        deriv = tmp_path / "derivatives"
        deriv.mkdir()
        (deriv / "instrument_config.yaml").write_text(":\n  - [\ninvalid")
        result = _resolve_instrument_from_yaml(tmp_path)
        assert result is None

    def test_yaml_missing_instrument_key(self, tmp_path):
        """YAML without instrument.id → None."""
        deriv = tmp_path / "derivatives"
        deriv.mkdir()
        (deriv / "instrument_config.yaml").write_text("other_key: value\n")
        result = _resolve_instrument_from_yaml(tmp_path)
        assert result is None

    def test_run_job_uses_yaml_resolution(self, tmp_path, v1_acq):
        """run_job resolves instrument from YAML when .json is missing."""
        (tmp_path / "acquisition.json").write_text(json.dumps(v1_acq))
        deriv = tmp_path / "derivatives"
        deriv.mkdir()
        (deriv / "instrument_config.yaml").write_text("instrument:\n  id: exaspim-01\n")

        mapper = ExaSPIMMapper()
        mapper.run_job(_make_job_settings(tmp_path))

        # instrument.json should now exist (written by run_job)
        inst_path = tmp_path / "instrument.json"
        assert inst_path.is_file()

        with open(tmp_path / "acquisition.json") as f:
            acq = json.load(f)
        from packaging import version

        assert version.parse(acq["schema_version"]) >= version.parse("2.0.0")
        assert "data_streams" in acq
