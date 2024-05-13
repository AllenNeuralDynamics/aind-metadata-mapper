"""Tests for the neuropixels open ephys rig ETL."""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_schema.core.rig import Rig  # type: ignore
from aind_metadata_mapper.dynamic_routing.open_ephys_rig import (
    OpenEphysRigEtl,
)

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "neuropixels"
)
BASE_RIG_PATH = RESOURCES_DIR / "base_rig.json"
OUTPUT_DIR = Path(".")  # File writes will be mocked


class TestOpenEphysRigEtl(unittest.TestCase):
    """Tests dxdiag utilities in for the neuropixels project."""

    def load_rig(self, model_path: Path):
        """Convenience function to load a rig model."""
        return Rig.model_validate_json(
            (model_path).read_text(),
        )

    def test_transform(self):
        """Tests etl transform."""
        expected = self.load_rig(RESOURCES_DIR / "open-ephys_rig.json")
        etl = OpenEphysRigEtl(
            BASE_RIG_PATH,
            OUTPUT_DIR,
            open_ephys_settings_sources=[
                RESOURCES_DIR / "settings.xml",
            ],
            probe_manipulator_serial_numbers=[
                (
                    "Ephys Assembly A",
                    "SN45356",
                ),
                (
                    "Ephys Assembly B",
                    "SN45484",
                ),
                (
                    "Ephys Assembly C",
                    "SN45485",
                ),
                (
                    "Ephys Assembly D",
                    "SN45359",
                ),
                (
                    "Ephys Assembly E",
                    "SN45482",
                ),
                (
                    "Ephys Assembly F",
                    "SN45361",
                ),
            ],
            modification_date=expected.modification_date,
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        self.assertEqual(transformed, expected)

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_etl(self, mock_write_standard_file: MagicMock):
        """Test ETL workflow."""
        etl = OpenEphysRigEtl(
            BASE_RIG_PATH,
            OUTPUT_DIR,
            open_ephys_settings_sources=[
                RESOURCES_DIR / "settings.xml",
            ],
            probe_manipulator_serial_numbers=[
                (
                    "Ephys Assembly A",
                    "SN45356",
                ),
                (
                    "Ephys Assembly B",
                    "SN45484",
                ),
                (
                    "Ephys Assembly C",
                    "SN45485",
                ),
                (
                    "Ephys Assembly D",
                    "SN45359",
                ),
                (
                    "Ephys Assembly E",
                    "SN45482",
                ),
                (
                    "Ephys Assembly F",
                    "SN45361",
                ),
            ],
        )
        etl.run_job()
        mock_write_standard_file.assert_called_once_with(
            output_directory=OUTPUT_DIR
        )

    def test_transform_no_update(self):
        """Tests etl transform when probe serial numbers dont change."""
        initial_rig_model_path = RESOURCES_DIR / "open-ephys_rig.json"
        etl = OpenEphysRigEtl(
            initial_rig_model_path,
            OUTPUT_DIR,
            open_ephys_settings_sources=[
                RESOURCES_DIR / "settings.xml",
            ],
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        initial_rig_model = self.load_rig(initial_rig_model_path)
        self.assertEqual(initial_rig_model.rig_id, transformed.rig_id)

    def test_transform_update_manipulators(self):
        """Tests etl transform when manipulator serial numbers change."""
        etl = OpenEphysRigEtl(
            BASE_RIG_PATH,
            OUTPUT_DIR,
            open_ephys_settings_sources=[],
            probe_manipulator_serial_numbers=[
                (
                    "Ephys Assembly A",
                    "SN45358",
                ),
            ],
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        initial_rig_model = self.load_rig(BASE_RIG_PATH)
        self.assertNotEqual(initial_rig_model.rig_id, transformed.rig_id)

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_etl_inferred_mapping(self, mock_write_standard_file: MagicMock):
        """Test ETL workflow with inferred probe mapping."""
        etl = OpenEphysRigEtl(
            BASE_RIG_PATH,
            OUTPUT_DIR,
            open_ephys_settings_sources=[
                RESOURCES_DIR / "settings.mislabeled-probes-0.xml",
                RESOURCES_DIR / "settings.mislabeled-probes-1.xml",
            ],
            probe_manipulator_serial_numbers=[
                (
                    "Ephys Assembly A",
                    "SN45356",
                ),
                (
                    "Ephys Assembly B",
                    "SN45484",
                ),
                (
                    "Ephys Assembly C",
                    "SN45485",
                ),
                (
                    "Ephys Assembly D",
                    "SN45359",
                ),
                (
                    "Ephys Assembly E",
                    "SN45482",
                ),
                (
                    "Ephys Assembly F",
                    "SN45361",
                ),
            ],
        )
        etl.run_job()
        mock_write_standard_file.assert_called_once_with(
            output_directory=OUTPUT_DIR
        )

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_etl_inferred_mapping_mismatched_probe_count(
        self, mock_write_standard_file: MagicMock
    ):
        """Test ETL workflow with mismatched probe count."""
        etl = OpenEphysRigEtl(
            RESOURCES_DIR / "base-missing-probe_rig.json",
            OUTPUT_DIR,
            open_ephys_settings_sources=[
                RESOURCES_DIR / "settings.mislabeled-probes-0.xml",
                RESOURCES_DIR / "settings.mislabeled-probes-1.xml",
            ],
            probe_manipulator_serial_numbers=[
                (
                    "Ephys Assembly A",
                    "SN45356",
                ),
                (
                    "Ephys Assembly B",
                    "SN45484",
                ),
                (
                    "Ephys Assembly C",
                    "SN45485",
                ),
                (
                    "Ephys Assembly D",
                    "SN45359",
                ),
                (
                    "Ephys Assembly E",
                    "SN45482",
                ),
                (
                    "Ephys Assembly F",
                    "SN45361",
                ),
            ],
        )
        etl.run_job()
        mock_write_standard_file.assert_called_once_with(
            output_directory=OUTPUT_DIR
        )


if __name__ == "__main__":
    unittest.main()
