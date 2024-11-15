"""Unit tests for mesoscope etl package"""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.core.session import Session
from PIL import Image

from aind_metadata_mapper.mesoscope.models import JobSettings
from aind_metadata_mapper.mesoscope.session import MesoscopeEtl

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "mesoscope"
)
STIMULUS_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "stimulus"
)

EXAMPLE_MOVIE_META = RESOURCES_DIR / "example_movie_meta.json"
EXAMPLE_SESSION = RESOURCES_DIR / "expected_session.json"
EXAMPLE_SESSION_META = RESOURCES_DIR / "example_session_meta.json"
EXAMPLE_PLATFORM = RESOURCES_DIR / "example_platform.json"
EXAMPLE_TIMESERIES = RESOURCES_DIR / "example_timeseries_meta.json"
EXAMPLE_SESSION_META = RESOURCES_DIR / "example_session_meta.json"
EXAMPLE_IMAGE = RESOURCES_DIR / "test.tiff"
USER_INPUT = RESOURCES_DIR / "user_input.json"
CAMSTIM_INPUT = STIMULUS_DIR / "camstim_input.json"


class TestMesoscope(unittest.TestCase):
    """Tests methods in MesoscopeEtl class"""

    maxDiff = None  # show full diff without truncation

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the test suite"""
        with open(EXAMPLE_MOVIE_META, "r") as f:
            cls.example_movie_meta = json.load(f)
        with open(EXAMPLE_SESSION, "r") as f:
            expected_session = json.load(f)
        with open(EXAMPLE_PLATFORM, "r") as f:
            cls.example_platform = json.load(f)
        with open(EXAMPLE_TIMESERIES, "r") as f:
            cls.example_timeseries_meta = json.load(f)
        with open(EXAMPLE_SESSION_META, "r") as f:
            cls.example_session_meta = json.load(f)
        expected_session["schema_version"] = Session.model_fields[
            "schema_version"
        ].default
        cls.example_session = expected_session
        cls.example_scanimage_meta = {
            "lines_per_frame": 512,
            "pixels_per_line": 512,
            "fov_scale_factor": 1.0,
        }
        with open(USER_INPUT, "r") as f:
            cls.user_input = json.load(f)
        with open(CAMSTIM_INPUT, "r") as f:
            cls.camstim_input = json.load(f)

    @patch("pathlib.Path.is_dir")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.__init__")
    def test_constructor_from_string(
        self,
        mock_camstim: MagicMock,
        mock_is_dir: MagicMock,
    ) -> None:
        """Tests that the settings can be constructed from a json string"""
        mock_camstim.return_value = None
        mock_is_dir.return_value = True
        job_settings = JobSettings(**self.user_input)
        job_settings_str = job_settings.model_dump_json()
        etl = MesoscopeEtl(
            job_settings=job_settings_str,
        )
        self.assertEqual(etl.job_settings, JobSettings(**self.user_input))

    @patch("pathlib.Path.is_file")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.__init__")
    @patch("pathlib.Path.is_dir")
    @patch("builtins.open", mock_open(read_data="test data"))
    def test_read_metadata_value_error(
        self,
        mock_is_dir: MagicMock,
        mock_camstim: MagicMock,
        mock_is_file: MagicMock,
    ) -> None:
        """Tests that _read_metadata raises a ValueError"""
        mock_is_dir.return_value = True
        mock_camstim.return_value = None
        mock_is_file.return_value = False
        etl1 = MesoscopeEtl(
            job_settings=JobSettings(**self.user_input),
        )
        tiff_path = Path("non_existent_file_path")
        with self.assertRaises(ValueError):
            etl1._read_metadata(tiff_path)

    @patch("pathlib.Path.is_file")
    @patch("builtins.open")
    @patch("tifffile.FileHandle")
    @patch("tifffile.read_scanimage_metadata")
    @patch("pathlib.Path.is_dir")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.__init__")
    def test_read_metadata(
        self,
        mock_camstim: MagicMock,
        mock_is_dir: MagicMock,
        mock_read_scan: MagicMock,
        mock_file_handle: MagicMock,
        mock_open: MagicMock,
        mock_is_file: MagicMock,
    ) -> None:
        """Tests that _read_metadata calls readers"""
        mock_camstim.return_value = None
        mock_is_dir.return_value = True
        mock_is_file.return_value = True
        etl1 = MesoscopeEtl(
            job_settings=JobSettings(**self.user_input),
        )
        tiff_path = Path("file_path")
        etl1._read_metadata(tiff_path)
        mock_open.assert_called()
        mock_file_handle.assert_called()
        mock_read_scan.assert_called()

    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.rglob")
    @patch("pathlib.Path.glob")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.__init__")
    @patch(
        "aind_metadata_mapper.mesoscope.session.MesoscopeEtl._extract_platform_metadata"  # noqa
    )
    @patch(
        "aind_metadata_mapper.mesoscope.session.MesoscopeEtl._extract_time_series_metadata"  # noqa
    )
    def test_extract(
        self,
        mock_extract_timeseries: MagicMock,
        mock_platform: MagicMock,
        mock_camstim: MagicMock,
        mock_glob: MagicMock,
        mock_rglob: MagicMock,
        mock_is_dir: MagicMock,
    ) -> None:
        """Tests that the raw image info is extracted correctly."""
        mock_extract_timeseries.return_value = self.example_movie_meta
        mock_platform.return_value = self.example_platform
        mock_camstim.return_value = None
        mock_glob.return_value = iter([Path("somedir/a")])
        mock_rglob.return_value = iter([Path("somedir/a")])
        mock_is_dir.return_value = True
        etl = MesoscopeEtl(
            job_settings=JobSettings(**self.user_input),
        )

        session_meta, movie_meta = etl._extract()
        self.assertEqual(movie_meta, self.example_movie_meta)
        self.assertEqual(session_meta, self.example_platform)

    @patch("pathlib.Path.is_dir")
    @patch(
        "aind_metadata_mapper.mesoscope.session.MesoscopeEtl._extract_platform_metadata"  # noqa
    )
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.__init__")
    @patch(
        "aind_metadata_mapper.mesoscope.session.MesoscopeEtl._extract_time_series_metadata"  # noqa
    )
    def test_model(
        self,
        mock_extract_movie: MagicMock,
        mock_camstim: MagicMock,
        mock_extract_platform: MagicMock,
        mock_is_dir: MagicMock,
    ) -> None:
        """Tests that _extract raises a ValueError"""
        mock_extract_movie.return_value = self.example_movie_meta
        mock_camstim.return_value = None
        mock_extract_platform.return_value = self.example_platform
        mock_is_dir.return_value = False
        with self.assertRaises(ValueError):
            JobSettings(**self.user_input)

    @patch(
        "aind_metadata_mapper.mesoscope.session.MesoscopeEtl._read_metadata"
    )
    @patch("PIL.Image.open")
    @patch("pathlib.Path.is_dir")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.__init__")
    @patch(
        "aind_metadata_mapper.mesoscope.session.MesoscopeEtl._camstim_epoch_and_session"  # noqa
    )
    def test_transform(
        self,
        mock_camstim_epochs: MagicMock,
        mock_camstim: MagicMock,
        mock_dir: MagicMock,
        mock_open: MagicMock,
        mock_scanimage: MagicMock,
    ) -> None:
        """Tests that the platform json is extracted and transfromed into a
        session object correctly"""
        mock_camstim_epochs.return_value = ([], "ANTERIOR_MOUSEMOTION")
        mock_camstim.return_value = None
        mock_dir.return_value = True
        etl = MesoscopeEtl(
            job_settings=JobSettings(**self.user_input),
        )
        # mock vasculature image
        mock_image = Image.new("RGB", (100, 100))
        mock_image.tag = {306: ("2024:02:12 11:02:22",)}
        mock_open.return_value = mock_image

        mock_scanimage.return_value = self.example_scanimage_meta
        transformed_session = etl._transform(
            self.example_session_meta, self.example_timeseries_meta
        )
        self.assertEqual(
            self.example_session,
            json.loads(transformed_session.model_dump_json()),
        )

    @patch("aind_metadata_mapper.mesoscope.session.MesoscopeEtl._extract")
    @patch("aind_metadata_mapper.mesoscope.session.MesoscopeEtl._transform")
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch("pathlib.Path.is_dir")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.__init__")
    def test_run_job(
        self,
        mock_camstim: MagicMock,
        mock_is_dir: MagicMock,
        mock_write: MagicMock,
        mock_transform: MagicMock,
        mock_extract: MagicMock,
    ) -> None:
        """Tests the run_job method"""
        mock_camstim.return_value = None
        mock_is_dir.return_value = True
        mock_transform.return_value = Session.model_construct()
        mock_extract.return_value = (
            self.example_platform,
            self.example_movie_meta,
        )
        self.user_input["output_directory"] = str(RESOURCES_DIR)
        etl = MesoscopeEtl(
            job_settings=JobSettings(**self.user_input),
        )
        etl.run_job()
        mock_write.assert_called_once_with(output_directory=RESOURCES_DIR)


if __name__ == "__main__":
    unittest.main()
