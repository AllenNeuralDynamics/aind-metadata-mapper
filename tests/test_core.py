"""Tests class and methods in core module"""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_metadata_mapper.core import BaseJobSettings

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
CONFIG_FILE_PATH = RESOURCES_DIR / "job_settings.json"
CONFIG_FILE_PATH_CORRUPT = RESOURCES_DIR / "job_settings_corrupt.txt"


class TestJobSettings(unittest.TestCase):
    """Tests JobSettings can be configured from json file."""

    class MockJobSettings(BaseJobSettings):
        """Mock class for testing purposes"""

        name: str
        id: int

    def test_load_from_config_file(self):
        """Test job settings can be loaded from config file."""

        job_settings = self.MockJobSettings(
            user_settings_config_file=CONFIG_FILE_PATH
        )
        expected_settings_json = json.dumps(
            {
                "user_settings_config_file": str(CONFIG_FILE_PATH),
                "name": "Anna Apple",
                "id": 12345,
            }
        )
        round_trip = self.MockJobSettings.model_validate_json(
            expected_settings_json
        )
        self.assertEqual(
            round_trip.model_dump_json(), job_settings.model_dump_json()
        )

    @patch("logging.warning")
    def test_load_from_config_file_json_error(self, mock_log_warn: MagicMock):
        """Test job settings raises an error when config file is corrupt"""

        with self.assertRaises(Exception):
            self.MockJobSettings(
                user_settings_config_file=CONFIG_FILE_PATH_CORRUPT
            )
        mock_log_warn.assert_called_once()


if __name__ == "__main__":
    unittest.main()
