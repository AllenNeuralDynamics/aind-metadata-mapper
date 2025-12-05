""" "Unit tests for gather_metadata_registry.py."""

import json
import os
import tempfile
import unittest
from unittest import mock

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.base import MapperJob, MapperJobSettings
from aind_metadata_mapper.gather_metadata import GatherMetadataJob, JobSettings
from aind_metadata_mapper.mapper_registry import registry


class TestMapperJob(MapperJob):
    """A test mapper job that does nothing."""

    def run_job(self, job_settings: MapperJobSettings) -> None:
        """Run the test mapping job.

        This implementation does nothing.
        """
        raise NotImplementedError("This is a test mapper job and does not implement run_job.")


class TestGatherMetadataJob(unittest.TestCase):
    """Unit tests for GatherMetadataJob with registry."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for metadata files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.metadata_dir = self.temp_dir.name
        # Create a dummy input file for the mapper
        self.mapper_name = "test_mapper_job"
        self.input_filename = f"{self.mapper_name}.json"
        self.input_path = os.path.join(self.metadata_dir, self.input_filename)
        with open(self.input_path, "w") as f:
            json.dump({"dummy": "data"}, f)
        # Patch registry to include our test mapper
        registry[self.mapper_name] = TestMapperJob

    def tearDown(self):
        """Clean up test environment."""
        self.temp_dir.cleanup()

    @mock.patch.object(TestMapperJob, "run_job", autospec=True)
    @mock.patch("os.listdir")
    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("aind_metadata_mapper.gather_metadata.logging")
    def test_run_mappers_for_acquisition_registry_key(self, mock_logging, mock_exists, mock_listdir, mock_run_job):
        """Test _run_mappers_for_acquisition with a registry key."""
        # Patch os.listdir to return our test file
        mock_listdir.return_value = [self.input_filename]

        # Provide all required JobSettings fields
        settings = JobSettings(
            metadata_dir=self.metadata_dir,
            output_dir=self.metadata_dir,
            subject_id="test_subject",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
        )
        job = GatherMetadataJob(settings=settings)
        job._run_mappers_for_acquisition()
        # Check that run_job was called for the registry key
        mock_run_job.assert_called_once()
        args, kwargs = mock_run_job.call_args
        # The job_settings argument should have correct input/output paths
        self.assertEqual(str(args[1].input_filepath), str(self.input_path))
        self.assertTrue(str(args[1].output_filepath).endswith(f"acquisition_{self.mapper_name}.json"))

    @mock.patch("os.listdir")
    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("aind_metadata_mapper.gather_metadata.logging")
    def test_run_mappers_for_acquisition_registry_key_raises(self, mock_logging, mock_exists, mock_listdir):
        """Test _run_mappers_for_acquisition raises NotImplementedError for test mapper."""
        # Patch os.listdir to return our test file
        mock_listdir.return_value = [self.input_filename]

        settings = JobSettings(
            metadata_dir=self.metadata_dir,
            output_dir=self.metadata_dir,
            subject_id="test_subject",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            raise_if_mapper_errors=True,
        )
        job = GatherMetadataJob(settings=settings)
        with self.assertRaises(NotImplementedError):
            job._run_mappers_for_acquisition()

        settings.raise_if_mapper_errors = False
        job = GatherMetadataJob(settings=settings)
        # This should not raise now
        job._run_mappers_for_acquisition()


if __name__ == "__main__":
    registry["test_mapper_job"] = TestMapperJob
    unittest.main()
