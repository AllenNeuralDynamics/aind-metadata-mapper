"""Tests for the MVR rig ETL."""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_metadata_mapper.neuropixels.mvr_rig import MvrRigEtl  # type: ignore
from tests.test_neuropixels import utils as test_utils

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "neuropixels"
)


class TestMvrRigEtl(unittest.TestCase):
    """Tests dxdiag utilities in for the neuropixels project."""

    def test_transform(self):
        """Test etl transform."""
        etl = MvrRigEtl(
            self.input_source,
            self.output_dir,
            RESOURCES_DIR / "mvr.ini",
            mvr_mapping={
                "Camera 1": test_utils.SIDE_CAMERA_ASSEMBLY_NAME,
                "Camera 2": test_utils.EYE_CAMERA_ASSEMBLY_NAME,
                "Camera 3": test_utils.FORWARD_CAMERA_ASSEMBLY_NAME,
            },
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        self.assertEqual(transformed, self.expected)

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_run_job(self, mock_write_standard_file: MagicMock):
        """Test basic MVR etl workflow."""
        etl = MvrRigEtl(
            self.input_source,
            self.output_dir,
            RESOURCES_DIR / "mvr.ini",
            mvr_mapping={
                "Camera 1": test_utils.SIDE_CAMERA_ASSEMBLY_NAME,
                "Camera 2": test_utils.EYE_CAMERA_ASSEMBLY_NAME,
                "Camera 3": test_utils.FORWARD_CAMERA_ASSEMBLY_NAME,
            },
        )
        etl.run_job()
        mock_write_standard_file.assert_called_once_with(
            output_directory=self.output_dir
        )

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_run_job_bad_mapping(self, mock_write_standard_file: MagicMock):
        """Test MVR etl workflow with bad mapping."""
        etl = MvrRigEtl(
            self.input_source,
            self.output_dir,
            RESOURCES_DIR / "mvr.ini",
            mvr_mapping={
                "Camera 1": test_utils.SIDE_CAMERA_ASSEMBLY_NAME,
                "Camera 2": test_utils.EYE_CAMERA_ASSEMBLY_NAME,
                "Not a camera name": test_utils.FORWARD_CAMERA_ASSEMBLY_NAME,
            },
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
            RESOURCES_DIR / "mvr_rig.json",
        )


if __name__ == "__main__":
    unittest.main()
