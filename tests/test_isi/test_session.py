"""Unit tests for ISI ETL package"""

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import h5py as h5
import numpy as np
from aind_data_schema.core.session import (
    Session,
    StimulusEpoch,
    StimulusModality,
    Stream,
)
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.isi.models import JobSettings
from aind_metadata_mapper.isi.session import ISI


class TestISI(unittest.TestCase):
    """Tests methods in ISI ETL class"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.experimenter_name = ["John", "Doe"]
        self.subject_id = "test_subject_123"

        # Create mock job settings
        self.job_settings = JobSettings(
            input_source=self.temp_dir,
            experimenter_full_name=self.experimenter_name,
            subject_id=self.subject_id,
            output_directory=self.temp_dir,
            local_timezone="America/Los_Angeles",
        )

        # Create mock trial files
        self.trial_files = [
            self.temp_dir / "trial_001_trial1.hdf5",
            self.temp_dir / "trial_002_trial2.hdf5",
            self.temp_dir / "trial_003_trial3.hdf5",
        ]

        # Create mock HDF5 files
        for i, trial_file in enumerate(self.trial_files):
            with h5.File(trial_file, "w") as f:
                # Create mock timestamp data
                # 100 timestamps over 10+i seconds
                timestamps = np.linspace(0, 10 + i, 100)
                f.create_dataset("raw_images_timestamp", data=timestamps)

    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up temporary files
        for trial_file in self.trial_files:
            if trial_file.exists():
                trial_file.unlink()
        if self.temp_dir.exists():
            self.temp_dir.rmdir()

    def test_init_with_job_settings_object(self):
        """Test ISI initialization with JobSettings object"""
        with patch.object(
            ISI, "get_trial_files"
        ) as mock_get_trial_files, patch.object(
            ISI, "get_start_end_times"
        ) as mock_get_times:
            mock_get_trial_files.return_value = self.trial_files
            mock_get_times.return_value = (datetime.now(), datetime.now())

            isi = ISI(self.job_settings)

            self.assertEqual(isi.job_settings, self.job_settings)
            self.assertEqual(isi.trial_files, self.trial_files)
            mock_get_trial_files.assert_called_once()
            mock_get_times.assert_called_once()

    def test_init_with_json_string(self):
        """Test ISI initialization with JSON string"""
        job_settings_json = self.job_settings.model_dump_json()

        with patch.object(
            ISI, "get_trial_files"
        ) as mock_get_trial_files, patch.object(
            ISI, "get_start_end_times"
        ) as mock_get_times:
            mock_get_trial_files.return_value = self.trial_files
            mock_get_times.return_value = (datetime.now(), datetime.now())

            isi = ISI(job_settings_json)

            self.assertEqual(
                isi.job_settings.input_source, self.job_settings.input_source
            )
            self.assertEqual(
                isi.job_settings.experimenter_full_name,
                self.job_settings.experimenter_full_name,
            )
            self.assertEqual(
                isi.job_settings.subject_id, self.job_settings.subject_id
            )

    def test_get_trial_files_success(self):
        """Test successful retrieval of trial files"""
        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings

        result = isi.get_trial_files()

        self.assertEqual(len(result), 3)
        self.assertTrue(all(isinstance(f, Path) for f in result))
        self.assertTrue(all("trial" in f.name for f in result))

        # Verify files are sorted by creation time
        creation_times = [f.stat().st_ctime for f in result]
        self.assertEqual(creation_times, sorted(creation_times))

    def test_get_trial_files_no_trials_found(self):
        """Test error when no trial files are found"""
        empty_dir = Path(tempfile.mkdtemp())
        job_settings = JobSettings(
            input_source=empty_dir,
            experimenter_full_name=self.experimenter_name,
            subject_id=self.subject_id,
            local_timezone="America/Los_Angeles",
        )

        isi = ISI.__new__(ISI)
        isi.job_settings = job_settings

        with self.assertRaises(ValueError) as context:
            isi.get_trial_files()

        self.assertIn("No trials found", str(context.exception))
        empty_dir.rmdir()

    def test_get_start_end_times(self):
        """Test calculation of start and end times"""
        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings
        isi.trial_files = self.trial_files

        start_time, end_time = isi.get_start_end_times()

        self.assertIsInstance(start_time, datetime)
        self.assertIsInstance(end_time, datetime)
        self.assertLessEqual(start_time, end_time)

        # Verify start time is from first file
        tz = ZoneInfo("America/Los_Angeles")
        first_file_ctime = self.trial_files[0].stat().st_ctime
        expected_start = datetime.fromtimestamp(first_file_ctime, tz=tz)
        self.assertEqual(start_time, expected_start)

        # Verify end time is from last file
        last_file_ctime = self.trial_files[-1].stat().st_ctime
        expected_end = datetime.fromtimestamp(last_file_ctime, tz=tz)
        self.assertEqual(end_time, expected_end)

    def test_extract(self):
        """Test extraction of stimulus epochs from trial files"""
        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings
        isi.trial_files = self.trial_files

        stimulus_epochs = isi._extract()

        self.assertEqual(len(stimulus_epochs), 3)
        self.assertTrue(
            all(isinstance(epoch, StimulusEpoch) for epoch in stimulus_epochs)
        )

        for i, epoch in enumerate(stimulus_epochs):
            self.assertIsInstance(epoch.stimulus_start_time, datetime)
            self.assertIsInstance(epoch.stimulus_end_time, datetime)
            self.assertLessEqual(
                epoch.stimulus_start_time, epoch.stimulus_end_time
            )
            self.assertIn("trial", epoch.stimulus_name)
            self.assertEqual(
                epoch.stimulus_modalities, [StimulusModality.VISUAL]
            )

    @patch("h5py.File")
    def test_extract_with_mocked_hdf5(self, mock_h5_file):
        """Test extraction with mocked HDF5 file operations"""
        # Mock HDF5 file context manager
        mock_file = MagicMock()
        mock_h5_file.return_value.__enter__.return_value = mock_file
        mock_timestamps = np.array([0, 5, 10])
        # Mock the dataset access
        mock_dataset = MagicMock()
        mock_dataset.__call__ = MagicMock(return_value=mock_timestamps)
        mock_file.__getitem__.return_value = mock_dataset

        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings
        isi.trial_files = self.trial_files

        stimulus_epochs = isi._extract()

        self.assertEqual(len(stimulus_epochs), 3)
        self.assertEqual(mock_h5_file.call_count, 3)

    def test_transform(self):
        """Test transformation of stimulus epochs into Session model"""
        tz = ZoneInfo("America/Los_Angeles")
        start_time = datetime.now(tz=tz)
        end_time = start_time + timedelta(hours=1)

        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings
        isi.start_time = start_time
        isi.end_time = end_time

        # Create mock stimulus epochs
        stimulus_epochs = [
            StimulusEpoch(
                stimulus_start_time=start_time,
                stimulus_end_time=start_time + timedelta(minutes=10),
                stimulus_name="trial_001_trial1",
                stimulus_modalities=[StimulusModality.VISUAL],
            ),
            StimulusEpoch(
                stimulus_start_time=start_time + timedelta(minutes=15),
                stimulus_end_time=start_time + timedelta(minutes=25),
                stimulus_name="trial_002_trial2",
                stimulus_modalities=[StimulusModality.VISUAL],
            ),
        ]

        session = isi._transform(stimulus_epochs)

        self.assertIsInstance(session, Session)
        self.assertEqual(session.session_start_time, start_time)
        self.assertEqual(session.session_end_time, end_time)
        self.assertEqual(
            session.experimenter_full_name, self.experimenter_name
        )
        self.assertEqual(session.subject_id, self.subject_id)
        self.assertEqual(session.session_type, "ISI")
        self.assertEqual(session.rig_id, "ISI.1")
        self.assertEqual(session.mouse_platform_name, "disc")
        self.assertTrue(session.active_mouse_platform)

        # Check data streams
        self.assertEqual(len(session.data_streams), 1)
        stream = session.data_streams[0]
        self.assertIsInstance(stream, Stream)
        self.assertEqual(stream.stream_start_time, start_time)
        self.assertEqual(stream.stream_end_time, end_time)
        self.assertEqual(stream.stream_modalities, [Modality.ISI])
        self.assertEqual(stream.camera_names, ["Light source goes here XXX"])

        # Check stimulus epochs
        self.assertEqual(session.stimulus_epochs, stimulus_epochs)

    @patch.object(ISI, "_extract")
    @patch.object(ISI, "_transform")
    def test_run_job(self, mock_transform, mock_extract):
        """Test the complete run_job workflow"""
        # Setup mocks
        mock_epochs = [MagicMock()]
        mock_session = MagicMock()
        mock_extract.return_value = mock_epochs
        mock_transform.return_value = mock_session

        with patch.object(
            ISI, "get_trial_files"
        ) as mock_get_trial_files, patch.object(
            ISI, "get_start_end_times"
        ) as mock_get_times:
            mock_get_trial_files.return_value = self.trial_files
            mock_get_times.return_value = (datetime.now(), datetime.now())

            isi = ISI(self.job_settings)

            with patch("logging.info") as mock_logging:
                isi.run_job()

        # Verify the workflow
        mock_extract.assert_called_once()
        mock_transform.assert_called_once_with(mock_epochs)
        mock_session.write_standard_file.assert_called_once_with(
            output_directory=self.job_settings.output_directory
        )
        mock_logging.assert_called_once_with("Session loaded successfully.")

    def test_integration_with_real_files(self):
        """Integration test with actual HDF5 files (no mocking)"""
        with patch.object(ISI, "get_start_end_times") as mock_get_times:
            tz = ZoneInfo("America/Los_Angeles")
            start_time = datetime.now(tz=tz)
            end_time = start_time + timedelta(hours=1)
            mock_get_times.return_value = (start_time, end_time)

            isi = ISI(self.job_settings)

            # Test extraction
            stimulus_epochs = isi._extract()
            self.assertEqual(len(stimulus_epochs), 3)

            # Test transformation
            session = isi._transform(stimulus_epochs)
            self.assertIsInstance(session, Session)
            self.assertEqual(len(session.stimulus_epochs), 3)
            self.assertEqual(len(session.data_streams), 1)

    def test_trial_file_naming_convention(self):
        """Test that trial files follow expected naming convention"""
        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings

        trial_files = isi.get_trial_files()

        for trial_file in trial_files:
            self.assertIn("trial", trial_file.name.lower())
            self.assertTrue(trial_file.name.endswith(".hdf5"))

    def test_stimulus_epoch_timing_consistency(self):
        """Test that stimulus epoch timing is consistent with timestamps"""
        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings
        isi.trial_files = self.trial_files

        stimulus_epochs = isi._extract()

        for i, epoch in enumerate(stimulus_epochs):
            tz = ZoneInfo("America/Los_Angeles")
            file_ctime = self.trial_files[i].stat().st_ctime
            expected_start = datetime.fromtimestamp(file_ctime, tz=tz)
            self.assertEqual(epoch.stimulus_start_time, expected_start)

            # End time should be after start time
            self.assertGreater(
                epoch.stimulus_end_time, epoch.stimulus_start_time
            )

    def test_empty_trial_file_handling(self):
        """Test handling of trial files with unexpected structure"""
        # Create a trial file with missing timestamp data
        empty_trial = self.temp_dir / "empty_trial.hdf5"
        with h5.File(empty_trial, "w") as f:
            # Create file without raw_images_timestamp dataset
            f.create_dataset("dummy_data", data=[1, 2, 3])

        isi = ISI.__new__(ISI)
        isi.job_settings = self.job_settings
        isi.trial_files = [empty_trial]

        with self.assertRaises(KeyError):
            isi._extract()

        empty_trial.unlink()


if __name__ == "__main__":
    unittest.main()
