"""Tests for the MVR rig ETL."""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_schema.core.rig import Rig

from aind_metadata_mapper.dynamic_routing.mvr_rig import (  # type: ignore
    MvrRigEtl,
)
from tests.test_dynamic_routing import test_utils as test_utils

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "dynamic_routing"
)

MVR_PATH = Path(RESOURCES_DIR / "mvr_rig.json")


class TestMvrRigEtl(unittest.TestCase):
    """Tests dxdiag utilities in for the dynamic_routing project."""

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
            modification_date=self.expected.modification_date,
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
        self.input_source = RESOURCES_DIR / "base_rig.json"
        self.output_dir = Path("abc")
        with open(MVR_PATH, "r") as f:
            mvr_contents = json.load(f)
        mvr_contents["schema_version"] = Rig.model_fields[
            "schema_version"
        ].default
        self.expected = Rig(**mvr_contents)


if __name__ == "__main__":
    unittest.main()
