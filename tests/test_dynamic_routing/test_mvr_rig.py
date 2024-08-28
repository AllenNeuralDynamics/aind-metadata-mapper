"""Tests for the MVR rig ETL."""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from aind_data_schema.core.rig import Rig 
from aind_metadata_mapper.dynamic_routing.mvr_rig import (  # type: ignore
    MvrRigEtl,
)
from typing import Tuple

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "dynamic_routing"
)

FORWARD_CAMERA_ASSEMBLY_NAME = "Forward"
FORWARD_CAMERA_NAME = f"{FORWARD_CAMERA_ASSEMBLY_NAME} camera"
EYE_CAMERA_ASSEMBLY_NAME = "Eye"
EYE_CAMERA_NAME = f"{EYE_CAMERA_ASSEMBLY_NAME} camera"
SIDE_CAMERA_ASSEMBLY_NAME = "Side"
SIDE_CAMERA_NAME = f"{SIDE_CAMERA_ASSEMBLY_NAME} camera"


def setup_neuropixels_etl_resources(
    expected_json: Path,
) -> Tuple[Path, Path, Rig]:
    """Sets test resources dynamic_routing etl.

    Parameters
    ----------
    expected_json: Path
      paths to etl resources to move to input dir

    Returns
    -------
    Tuple[Path, Path, Rig]
      input_source: path to etl base rig input source
      output_dir: path to etl output directory
      expected_rig: rig model to compare to output
    """
    return (
        RESOURCES_DIR / "base_rig.json",
        Path("abc"),  # hopefully file writes are mocked
        Rig.model_validate_json(expected_json.read_text()),
    )

class TestMvrRigEtl(unittest.TestCase):
    """Tests dxdiag utilities in for the dynamic_routing project."""

    def test_transform(self):
        """Test etl transform."""
        etl = MvrRigEtl(
            self.input_source,
            self.output_dir,
            RESOURCES_DIR / "mvr.ini",
            mvr_mapping={
                "Camera 1": SIDE_CAMERA_ASSEMBLY_NAME,
                "Camera 2": EYE_CAMERA_ASSEMBLY_NAME,
                "Camera 3": FORWARD_CAMERA_ASSEMBLY_NAME,
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
                "Camera 1": SIDE_CAMERA_ASSEMBLY_NAME,
                "Camera 2": EYE_CAMERA_ASSEMBLY_NAME,
                "Camera 3": FORWARD_CAMERA_ASSEMBLY_NAME,
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
                "Camera 1": SIDE_CAMERA_ASSEMBLY_NAME,
                "Camera 2": EYE_CAMERA_ASSEMBLY_NAME,
                "Not a camera name": FORWARD_CAMERA_ASSEMBLY_NAME,
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
        ) = setup_neuropixels_etl_resources(
            RESOURCES_DIR / "mvr_rig.json",
        )


if __name__ == "__main__":
    unittest.main()
