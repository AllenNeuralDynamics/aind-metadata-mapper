"""Unit tests for mesoscope etl package"""

import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from aind_metadata_mapper.mesoscope.session import JobSettings, MesoscopeEtl

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / "resources"
    / "mesoscope"
)

EXAMPLE_EXTRACT = RESOURCES_DIR / "example_extract.json"
EXAMPLE_SESSION = RESOURCES_DIR / "expected_session.json"
EXAMPLE_PLATFORM = RESOURCES_DIR / "example_platform.json"
EXAMPLE_IMAGE = RESOURCES_DIR / "test.tiff"


class TestMesoscope(unittest.TestCase):
    """Tests methods in MesoscopeEtl class"""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the test suite"""
        with open(EXAMPLE_EXTRACT, "r") as f:
            cls.example_extract = json.load(f)
        with open(EXAMPLE_SESSION, "r") as f:
            cls.example_session = json.load(f)
        cls.example_scanimage_meta = {
            "lines_per_frame": 512,
            "pixels_per_line": 512,
            "fov_scale_factor": 1.0,
        }
        cls.example_job_settings = JobSettings(
            input_source=EXAMPLE_PLATFORM,
            behavior_source=RESOURCES_DIR,
            output_directory=RESOURCES_DIR,
            subject_id="12345",
            session_start_time=datetime(2024, 2, 22, 15, 30, 0),
            session_end_time=datetime(2024, 2, 22, 17, 30, 0),
            project="some_project",
            experimenter_full_name=["John Doe"],
            magnification="16x",
            fov_coordinate_ap=1.5,
            fov_coordinate_ml=1.5,
            fov_reference="Bregma",
            iacuc_protocol="12345",
            mouse_platform_name="disc",
        )

    def test_extract(self) -> None:
        """Tests that the raw image info is extracted correcetly."""
        etl = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        with open(EXAMPLE_EXTRACT, "r") as f:
            expected_extract = json.load(f)
        extract = etl._extract()
        self.assertEqual(extract, expected_extract)

    @patch(
        "aind_metadata_mapper.mesoscope.session.MesoscopeEtl._read_metadata"
    )
    @patch("PIL.Image.open")
    def test_transform(self, mock_open, mock_scanimage) -> None:
        """Tests that the platform json is extracted and transfromed into a
        session object correctly"""
        etl = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        # mock vasculature image
        mock_image = Image.new("RGB", (100, 100))
        mock_image.tag = {306: ("2024:02:12 11:02:22",)}
        mock_open.return_value = mock_image

        # mock scanimage metadata
        mock_meta = [{}]
        mock_meta[0][
            "SI.hRoiManager.linesPerFrame"
        ] = self.example_scanimage_meta["lines_per_frame"]
        mock_meta[0][
            "SI.hRoiManager.pixelsPerLine"
        ] = self.example_scanimage_meta["pixels_per_line"]
        mock_meta[0][
            "SI.hRoiManager.scanZoomFactor"
        ] = self.example_scanimage_meta["fov_scale_factor"]
        mock_scanimage.return_value = mock_meta

        extract = etl._extract()
        transformed_session = etl._transform(extract)

        self.assertEqual(
            self.example_session,
            json.loads(transformed_session.model_dump_json())
        )


if __name__ == "__main__":
    unittest.main()