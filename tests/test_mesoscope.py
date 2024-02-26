import unittest
from unittest.mock import patch
import os
from pathlib import Path
import json
from datetime import datetime
from PIL import Image

from aind_metadata_mapper.mesoscope.session import (
    MesoscopeEtl,
    UserSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources" / "mesoscope"

EXAMPLE_EXTRACT = RESOURCES_DIR / "example_extract.json"
EXAMPLE_SESSION = RESOURCES_DIR / "expected_session.json"
EXAMPLE_PLATFORM = RESOURCES_DIR / "example_platform.json"


class TestMesoscope(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(EXAMPLE_EXTRACT, "r") as f:
            cls.example_extract = json.load(f)
        with open(EXAMPLE_SESSION, "r") as f:
            cls.example_session = json.load(f)
        cls.example_user_settings = UserSettings(
            mouse_id="12345",
            session_start_time=datetime(2024, 2, 22, 15, 30, 0),
            session_end_time=datetime(2024, 2, 22, 17, 30, 0),
            project="some_project",
        )

    def test_extract(self) -> None:
        """Tests that the raw image info is extracted correcetly."""
        etl = MesoscopeEtl(
            input_source=EXAMPLE_PLATFORM,
            behavior_source=RESOURCES_DIR,
            output_directory=RESOURCES_DIR,
            user_settings=self.example_user_settings,
        )
        with open(EXAMPLE_EXTRACT, "r") as f:
            expected_extract = json.load(f)
        extract = etl._extract()
        self.assertEqual(extract, expected_extract)

    @patch("PIL.Image.open")
    def test_transform(self, mock_open) -> None:
        """Tests that the platform json is extracted and transfromed into a session object correctly"""
        etl = MesoscopeEtl(
            input_source=EXAMPLE_PLATFORM,
            behavior_source=RESOURCES_DIR,
            output_directory=RESOURCES_DIR,
            user_settings=self.example_user_settings,
        )
        mock_image = Image.new("RGB", (100, 100))
        mock_image.tag = {306: ("2024:02:12 11:02:22",)}

        # Set up the mock to return the mock image when opened
        mock_open.return_value = mock_image
        extract = etl._extract()
        transformed_session = etl._transform(extract)
        self.assertEqual(transformed_session, self.example_session)


if __name__ == "__main__":
    unittest.main()
