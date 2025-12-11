"""Tests for base.py JobSettings and MapperJob"""

import unittest
from pathlib import Path

from aind_metadata_mapper.base import MapperJob, MapperJobSettings


class DummyMapper(MapperJob):
    """A dummy mapper for testing purposes"""

    def run_job(self, job_settings: MapperJobSettings) -> None:
        """Run the dummy mapping job."""
        assert isinstance(job_settings.input_filepath, Path)
        assert isinstance(job_settings.output_directory, Path)
        assert isinstance(job_settings.output_filename_suffix, str)
        self.was_run = True


class TestBaseModule(unittest.TestCase):
    """Tests for base.py module"""

    def test_jobsettings_fields(self):
        """Test that MapperJobSettings fields are set correctly"""
        input_fp = Path("/tmp/input.json")
        output_dir = Path("/tmp")
        output_suffix = "_test"
        settings = MapperJobSettings(
            input_filepath=input_fp,
            output_directory=output_dir,
            output_filename_suffix=output_suffix
        )
        self.assertEqual(settings.input_filepath, input_fp)
        self.assertEqual(settings.output_directory, output_dir)
        self.assertEqual(settings.output_filename_suffix, output_suffix)

    def test_mapperjob_not_implemented(self):
        """Test that MapperJob raises NotImplementedError"""
        settings = MapperJobSettings(
            input_filepath=Path("/tmp/in.json"),
            output_directory=Path("/tmp"),
            output_filename_suffix="_test"
        )
        mapper = MapperJob()
        with self.assertRaises(NotImplementedError):
            mapper.run_job(settings)

    def test_mapperjob_subclass(self):
        """Test that a subclass of MapperJob can run without error"""
        input_fp = Path("/tmp/input.json")
        output_dir = Path("/tmp")
        output_suffix = "_test"
        settings = MapperJobSettings(
            input_filepath=input_fp,
            output_directory=output_dir,
            output_filename_suffix=output_suffix
        )
        mapper = DummyMapper()
        mapper.was_run = False
        mapper.run_job(settings)
        self.assertTrue(mapper.was_run)


if __name__ == "__main__":
    unittest.main()
