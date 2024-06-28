"""Unit tests for mesoscope etl package"""

import json
import os
import unittest
import zoneinfo
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_schema.core.session import Session
from PIL import Image

from aind_metadata_mapper.mesoscope.session import JobSettings, MesoscopeEtl

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "mesoscope"
)

EXAMPLE_EXTRACT = RESOURCES_DIR / "example_extract.json"
EXAMPLE_SESSION = RESOURCES_DIR / "expected_session.json"
EXAMPLE_PLATFORM = RESOURCES_DIR / "example_platform.json"
EXAMPLE_IMAGE = RESOURCES_DIR / "test.tiff"


class TestMesoscope(unittest.TestCase):
    """Tests methods in MesoscopeEtl class"""

    maxDiff = None  # show full diff without truncation

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
            session_start_time=datetime(
                2024, 2, 22, 15, 30, 0, tzinfo=zoneinfo.ZoneInfo("UTC")
            ),
            session_end_time=datetime(
                2024, 2, 22, 17, 30, 0, tzinfo=zoneinfo.ZoneInfo("UTC")
            ),
            project="some_project",
            experimenter_full_name=["John Doe"],
            magnification="16x",
            fov_coordinate_ap=1.5,
            fov_coordinate_ml=1.5,
            fov_reference="Bregma",
            iacuc_protocol="12345",
            mouse_platform_name="disc",
        )

    def test_constructor_from_string(self) -> None:
        """Tests that the settings can be constructed from a json string"""
        job_settings_str = self.example_job_settings.model_dump_json()
        etl0 = MesoscopeEtl(
            job_settings=job_settings_str,
        )
        etl1 = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        self.assertEqual(etl1.job_settings, etl0.job_settings)

    @patch("pathlib.Path.is_file")
    def test_read_metadata_value_error(self, mock_is_file: MagicMock) -> None:
        """Tests that _read_metadata raises a ValueError"""
        mock_is_file.return_value = False
        etl1 = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        tiff_path = Path("non_existent_file_path")
        with self.assertRaises(ValueError) as e:
            etl1._read_metadata(tiff_path)
        self.assertEqual(
            f"{tiff_path.resolve().absolute()} is not a file",
            e.exception.args[0],
        )

    @patch("pathlib.Path.is_file")
    @patch("builtins.open")
    @patch("tifffile.FileHandle")
    @patch("tifffile.read_scanimage_metadata")
    def test_read_metadata(
        self,
        mock_read_scan: MagicMock,
        mock_file_handle: MagicMock,
        mock_open: MagicMock,
        mock_is_file: MagicMock,
    ) -> None:
        """Tests that _read_metadata calls readers"""
        mock_is_file.return_value = True
        etl1 = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        tiff_path = Path("file_path")
        etl1._read_metadata(tiff_path)
        mock_open.assert_called()
        mock_file_handle.assert_called()
        mock_read_scan.assert_called()

    def test_extract(self) -> None:
        """Tests that the raw image info is extracted correctly."""
        etl = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        with open(EXAMPLE_EXTRACT, "r") as f:
            expected_extract = json.load(f)
        extract = etl._extract()
        self.assertEqual(extract, expected_extract)

    @patch("pathlib.Path.is_dir")
    def test_extract_no_behavior_dir(self, mock_is_dir: MagicMock) -> None:
        """Tests that _extract raises a ValueError"""
        mock_is_dir.return_value = False
        etl1 = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        with self.assertRaises(ValueError) as e:
            etl1._extract()
        self.assertEqual(
            "Behavior source must be a directory",
            e.exception.args[0],
        )

    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    def test_extract_no_input_source(
        self,
        mock_path_glob: MagicMock,
        mock_path_exists: MagicMock,
        mock_is_dir: MagicMock,
    ) -> None:
        """Tests that _extract raises a ValueError"""
        mock_is_dir.return_value = True
        mock_path_exists.return_value = False
        mock_path_glob.return_value = iter([Path("/somedir/a")])
        etl1 = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        with self.assertRaises(ValueError) as e:
            etl1._extract()
        self.assertEqual(
            "No platform json file found in directory",
            e.exception.args[0],
        )

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
        mock_meta[0]["SI.hRoiManager.linesPerFrame"] = (
            self.example_scanimage_meta["lines_per_frame"]
        )
        mock_meta[0]["SI.hRoiManager.pixelsPerLine"] = (
            self.example_scanimage_meta["pixels_per_line"]
        )
        mock_meta[0]["SI.hRoiManager.scanZoomFactor"] = (
            self.example_scanimage_meta["fov_scale_factor"]
        )
        mock_scanimage.return_value = mock_meta

        extract = etl._extract()
        transformed_session = etl._transform(extract)
        for stream in transformed_session.data_streams:
            stream.stream_start_time = stream.stream_start_time.replace(
                tzinfo=zoneinfo.ZoneInfo("UTC")
            )
            stream.stream_end_time = stream.stream_end_time.replace(
                tzinfo=zoneinfo.ZoneInfo("UTC")
            )
        self.assertEqual(
            self.example_session,
            json.loads(transformed_session.model_dump_json()),
        )

    @patch("aind_metadata_mapper.mesoscope.session.MesoscopeEtl._extract")
    @patch("aind_metadata_mapper.mesoscope.session.MesoscopeEtl._transform")
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_run_job(
        self,
        mock_write: MagicMock,
        mock_transform: MagicMock,
        mock_extract: MagicMock,
    ) -> None:
        """Tests the run_job method"""
        mock_transform.return_value = Session.model_construct()
        etl = MesoscopeEtl(
            job_settings=self.example_job_settings,
        )
        etl.run_job()
        mock_extract.assert_called_once()
        mock_write.assert_called_once_with(output_directory=RESOURCES_DIR)


if __name__ == "__main__":
    unittest.main()
