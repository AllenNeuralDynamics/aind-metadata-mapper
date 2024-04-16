"""Tests for the neuropixels open ephys rig ETL."""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_metadata_mapper.neuropixels.open_ephys_rig import (  # type: ignore
    OpenEphysRigEtl,
)
from tests.test_neuropixels import utils as test_utils

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "neuropixels"
)


class TestOpenEphysRigEtl(unittest.TestCase):
    """Tests dxdiag utilities in for the neuropixels project."""

    def test_transform(self):
        """Tests etl transform."""
        etl = OpenEphysRigEtl(
            self.input_source,
            self.output_dir,
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
            modification_date=self.expected.modification_date,
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        self.assertEqual(transformed, self.expected)

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_etl(self, mock_write_standard_file: MagicMock):
        """Test ETL workflow."""
        etl = OpenEphysRigEtl(
            self.input_source,
            self.output_dir,
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
            modification_date=self.expected.modification_date,
        )
        etl.run_job()
        mock_write_standard_file.assert_called_once_with(
            output_directory=self.output_dir
        )

    def setUp(self):
        """Sets up test resources."""
        (
            self.input_source,
            self.output_dir,
            self.expected,
        ) = test_utils.setup_neuropixels_etl_resources(
            RESOURCES_DIR / "open-ephys_rig.json",
        )


if __name__ == "__main__":
    unittest.main()
