"""Tests parsing of session information from open_ephys rig."""

# TODO: implement tests once np package issues are resolved

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aind_metadata_mapper.open_ephys.camstim_ephys_session import (
    CamstimEphysSessionEtl
)
from aind_metadata_mapper.open_ephys.models import (
    JobSettings as CamstimEphysJobSettings
)

RESOURCES_DIR = (
    Path(__file__).parent.parent / "resources" / "open_ephys"
)

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

    @patch('npc_ephys.openephys.get_single_oebin_path')
    def setUp(self, mock_oebin):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        mock_oebin.return_value = Path(self.temp_dir) / "fake.oebin"
        
        self.job_settings = CamstimEphysJobSettings(
            session_type="test_session",
            project_name="test_project",
            iacuc_protocol="test_protocol",
            description="test_description",
            mtrain_server="test_server",
            session_id="test_session_id",
            input_source=self.temp_dir
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
        self.assertTrue(hasattr(self.etl, 'job_settings'))

    def test_basic_attributes_exist(self):
        """Test that basic attributes exist."""
        attrs = ['job_settings', 'recording_dir']
        for attr in attrs:
            self.assertTrue(hasattr(self.etl, attr))


if __name__ == "__main__":
    unittest.main()