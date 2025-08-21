"""Tests methods in models module"""
import sys
import unittest
from unittest.mock import patch

from aind_metadata_mapper.models import JobSettings


class TestJobSettings(unittest.TestCase):
    """Tests JobSettings class."""

    def test_basic_constructor_from_args(self):
        """Tests basic constructor from command line args."""

        test_args = [
            "gather_metadata.py",
            "--metadata_dir",
            ".",
            "--subject_id",
            "12345",
        ]
        with patch.object(sys, "argv", test_args):
            job_settings = JobSettings()
        self.assertEqual(".", job_settings.metadata_dir)
        self.assertEqual("12345", job_settings.subject_id)
        self.assertIsNone(job_settings.instrument_id)


if __name__ == "__main__":
    unittest.main()
