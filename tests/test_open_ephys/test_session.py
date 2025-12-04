"""Tests parsing of session information from open_ephys rig."""

# TODO: implement tests once np package issues are resolved

import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from aind_metadata_mapper.open_ephys.camstim_ephys_session import (
    CamstimEphysSessionEtl,
)
from aind_metadata_mapper.open_ephys.models import (
    JobSettings as CamstimEphysJobSettings,
)

RESOURCES_DIR = Path(__file__).parent.parent / "resources" / "open_ephys"

EXAMPLE_STAGE_LOGS = [
    RESOURCES_DIR / "newscale_main.csv",
    RESOURCES_DIR / "newscale_surface_finding.csv",
]
EXAMPLE_OPENEPHYS_LOGS = [
    RESOURCES_DIR / "settings_main.xml",
    RESOURCES_DIR / "settings_surface_finding.xml",
]

EXPECTED_SESSION = RESOURCES_DIR / "ephys_session.json"

EXPECTED_CAMSTIM_JSON = RESOURCES_DIR / "camstim_ephys_session.json"


class TestCamstimEphysSessionEtl(unittest.TestCase):
    """Test methods in camstim ephys session module."""

    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.get_stage")
    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.get_fps")
    @patch("aind_metadata_mapper.open_ephys.utils.sync_utils.get_stop_time")
    @patch("aind_metadata_mapper.open_ephys.utils.sync_utils.get_start_time")
    @patch("aind_metadata_mapper.open_ephys.utils.sync_utils.load_sync")
    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.load_pkl")
    @patch("pandas.read_csv")
    @patch("json.loads")
    @patch("pathlib.Path.read_text")
    @patch("pathlib.Path.exists")
    @patch(
        "aind_metadata_mapper.open_ephys.camstim_ephys_session."
        "get_single_oebin_path"
    )
    @patch("pathlib.Path.rglob")
    def setUp(
        self,
        mock_rglob,
        mock_oebin,
        mock_exists,
        mock_read_text,
        mock_json_loads,
        mock_read_csv,
        mock_load_pkl,
        mock_load_sync,
        mock_start_time,
        mock_stop_time,
        mock_fps,
        mock_stage,
    ):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

        # Configure all mocks
        mock_oebin.return_value = Path(self.temp_dir) / "fake.oebin"
        mock_exists.return_value = True
        mock_read_text.return_value = '{"project": "test_project"}'
        mock_json_loads.return_value = {
            "project": "test_project",
            "operatorID": "test.operator",
            "rig_id": "test_rig",
            "InsertionNotes": {},
        }

        # Mock rglob
        def rglob_side_effect(pattern):
            """
            Returns temp files for rglob.
            """
            if pattern == "*.stim.pkl":
                return [Path(self.temp_dir) / "session.stim.pkl"]
            elif pattern == "*.opto.pkl":
                return []
            elif pattern.endswith(".sync"):
                return [Path(self.temp_dir) / "session.sync"]
            elif "_platform" in pattern:
                return [Path(self.temp_dir) / "session_platform.json"]
            else:
                return []

        mock_rglob.side_effect = rglob_side_effect

        # pandas.read_csv
        mock_read_csv.return_value = pd.DataFrame(
            {
                "Start": [0.0, 10.0, 20.0],
                "End": [5.0, 15.0, 25.0],
                "start_time": [0.0, 10.0, 20.0],
                "stop_time": [5.0, 15.0, 25.0],
                "stim_name": ["stim1", "stim2", "stim3"],
            }
        )

        mock_load_pkl.return_value = {
            "fps": 60,
            "stage": "test_stage",
            "session_uuid": "test-session-uuid-123",
        }
        mock_load_sync.return_value = None
        mock_start_time.return_value = datetime(2023, 1, 1, 10, 0, 0)
        mock_stop_time.return_value = datetime(2023, 1, 1, 12, 0, 0)
        mock_fps.return_value = 60
        mock_stage.return_value = "test_stage"

        self.job_settings = CamstimEphysJobSettings(
            session_type="test_session",
            project_name="test_project",
            iacuc_protocol="test_protocol",
            description="test_description",
            mtrain_server="test_server",
            session_id="test_session_id",
            input_source=self.temp_dir,
        )
        self.etl = CamstimEphysSessionEtl(job_settings=self.job_settings)

    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init(self):
        """Test initialization."""
        self.assertIsInstance(self.etl, CamstimEphysSessionEtl)

    def test_has_job_settings(self):
        """Test that job_settings attribute exists."""
        self.assertTrue(hasattr(self.etl, "job_settings"))

    def test_basic_attributes_exist(self):
        """Test that basic attributes exist."""
        attrs = ["job_settings", "recording_dir"]
        for attr in attrs:
            self.assertTrue(hasattr(self.etl, attr))


if __name__ == "__main__":
    unittest.main()
