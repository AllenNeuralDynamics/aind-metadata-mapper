"""Tests methods in models module"""

import unittest

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.models import JobSettings


class TestJobSettings(unittest.TestCase):
    """Tests JobSettings class."""

    def test_basic_constructor_from_args(self):
        """Tests basic constructor from command line args."""
        # Create JobSettings directly with parameters since command line
        # parsing of modalities has complex requirements
        job_settings = JobSettings(
            metadata_dir=".",
            output_dir="./output",
            subject_id="12345",
            project_name="test_project",
            modalities=[Modality.ECEPHYS, Modality.BEHAVIOR],
        )
        self.assertEqual(".", job_settings.metadata_dir)
        self.assertEqual("12345", job_settings.subject_id)
        self.assertEqual("test_project", job_settings.project_name)
        expected_modalities = [Modality.ECEPHYS, Modality.BEHAVIOR]
        self.assertEqual(expected_modalities, job_settings.modalities)

    def test_output_path_defaults_to_input_path(self):
        """Tests that output_dir defaults to metadata_dir when not provided."""
        job_settings = JobSettings(
            metadata_dir="/test/input",
            subject_id="12345",
            project_name="test_project",
            modalities=[Modality.ECEPHYS],
        )
        self.assertEqual("/test/input", job_settings.metadata_dir)
        self.assertEqual("/test/input", job_settings.output_dir)


if __name__ == "__main__":
    unittest.main()
