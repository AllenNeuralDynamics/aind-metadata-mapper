"""Unit tests for ISI ETL package"""

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from aind_data_schema.core.session import (
    LightEmittingDiodeConfig,
    Session,
    StimulusEpoch,
    StimulusModality,
    Stream,
    VisualStimulation,
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

        # Create session times for the new JobSettings structure
        tz = ZoneInfo("America/Los_Angeles")
        self.session_start_time = datetime.now(tz=tz)
        self.session_end_time = self.session_start_time + timedelta(hours=1)

        # Create mock job settings with required session times
        self.job_settings = JobSettings(
            input_source=self.temp_dir,
            experimenter_full_name=self.experimenter_name,
            subject_id=self.subject_id,
            output_directory=self.temp_dir,
            local_timezone="America/Los_Angeles",
            session_start_time=self.session_start_time,
            session_end_time=self.session_end_time,
        )

        # No longer need to create mock trial files since the new
        # implementation doesn't read from HDF5 files

    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up temporary directory
        if self.temp_dir.exists():
            self.temp_dir.rmdir()

    def test_init_with_job_settings_object(self):
        """Test ISI initialization with JobSettings object"""
        isi = ISI(self.job_settings)

        self.assertEqual(isi.job_settings, self.job_settings)

    def test_init_with_json_string(self):
        """Test ISI initialization with JSON string"""
        job_settings_json = self.job_settings.model_dump_json()

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

    def test_extract(self):
        """Test extraction of stimulus epochs"""
        isi = ISI(self.job_settings)

        stimulus_epochs = isi._extract()

        self.assertEqual(len(stimulus_epochs), 1)
        self.assertIsInstance(stimulus_epochs[0], StimulusEpoch)

        epoch = stimulus_epochs[0]
        self.assertEqual(epoch.stimulus_start_time, self.session_start_time)
        self.assertEqual(epoch.stimulus_end_time, self.session_end_time)
        self.assertEqual(epoch.stimulus_name, "IntrinsicStim")
        self.assertEqual(epoch.stimulus_modalities, [StimulusModality.VISUAL])

        # Check that stimulus_parameters includes VisualStimulation object
        self.assertIsNotNone(epoch.stimulus_parameters)
        self.assertEqual(len(epoch.stimulus_parameters), 1)
        self.assertIsInstance(epoch.stimulus_parameters[0], VisualStimulation)
        self.assertEqual(
            epoch.stimulus_parameters[0].stimulus_name,
            "DriftingCheckerboardBar",
        )

    def test_transform(self):
        """Test transformation of stimulus epochs into Session model"""
        isi = ISI(self.job_settings)

        # Create mock stimulus epochs matching the new format
        stimulus_epochs = [
            StimulusEpoch(
                stimulus_start_time=self.session_start_time,
                stimulus_end_time=self.session_end_time,
                stimulus_name="IntrinsicStim",
                stimulus_modalities=[StimulusModality.VISUAL],
                stimulus_parameters=[
                    VisualStimulation(stimulus_name="DriftingCheckerboardBar")
                ],
            )
        ]

        session = isi._transform(stimulus_epochs)

        self.assertIsInstance(session, Session)
        self.assertEqual(session.session_start_time, self.session_start_time)
        self.assertEqual(session.session_end_time, self.session_end_time)
        self.assertEqual(
            session.experimenter_full_name, self.experimenter_name
        )
        self.assertEqual(session.subject_id, self.subject_id)
        self.assertEqual(session.session_type, "ISI")
        # rig_id now comes from environment variable
        self.assertIsInstance(session.rig_id, str)
        self.assertEqual(session.mouse_platform_name, "disc")
        self.assertTrue(session.active_mouse_platform)

        # Check data streams
        self.assertEqual(len(session.data_streams), 1)
        stream = session.data_streams[0]
        self.assertIsInstance(stream, Stream)
        self.assertEqual(stream.stream_start_time, self.session_start_time)
        self.assertEqual(stream.stream_end_time, self.session_end_time)
        self.assertEqual(stream.stream_modalities, [Modality.ISI])

        # Check light sources
        self.assertEqual(len(stream.light_sources), 1)
        light_source = stream.light_sources[0]
        self.assertIsInstance(light_source, LightEmittingDiodeConfig)
        self.assertEqual(light_source.name, "ISI LED")

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

    def test_integration_workflow(self):
        """Integration test for the complete workflow"""
        isi = ISI(self.job_settings)

        # Test extraction
        stimulus_epochs = isi._extract()
        self.assertEqual(len(stimulus_epochs), 1)
        self.assertEqual(
            stimulus_epochs[0].stimulus_name, "IntrinsicStim"
        )

        # Test transformation
        session = isi._transform(stimulus_epochs)
        self.assertIsInstance(session, Session)
        self.assertEqual(len(session.stimulus_epochs), 1)
        self.assertEqual(len(session.data_streams), 1)

        # Verify session uses JobSettings timing
        self.assertEqual(session.session_start_time, self.session_start_time)
        self.assertEqual(session.session_end_time, self.session_end_time)

    def test_stimulus_parameters_structure(self):
        """Test that stimulus parameters have expected structure"""
        isi = ISI(self.job_settings)
        stimulus_epochs = isi._extract()

        epoch = stimulus_epochs[0]
        self.assertIsNotNone(epoch.stimulus_parameters)
        self.assertEqual(len(epoch.stimulus_parameters), 1)

        visual_stim = epoch.stimulus_parameters[0]
        self.assertIsInstance(visual_stim, VisualStimulation)
        self.assertEqual(visual_stim.stimulus_name, "DriftingCheckerboardBar")

        # Check that stimulus_parameters has expected attributes
        # The parameters are stored as an AindGeneric object, not a dict
        params = visual_stim.stimulus_parameters
        self.assertTrue(hasattr(params, "window"))
        self.assertTrue(hasattr(params, "size"))
        self.assertTrue(hasattr(params, "ori"))
        self.assertTrue(hasattr(params, "contrast"))

        # Verify some specific parameter values
        self.assertEqual(params.size, [1920, 1200])
        self.assertEqual(params.ori, [0, 90, 180, 270])
        self.assertEqual(params.contrast, 1.0)

    def test_session_metadata_consistency(self):
        """Test that session metadata is consistent throughout"""
        isi = ISI(self.job_settings)

        stimulus_epochs = isi._extract()
        session = isi._transform(stimulus_epochs)

        # Verify timing consistency
        self.assertEqual(
            session.session_start_time, self.job_settings.session_start_time
        )
        self.assertEqual(
            session.session_end_time, self.job_settings.session_end_time
        )

        # Verify stream timing matches session timing
        stream = session.data_streams[0]
        self.assertEqual(stream.stream_start_time, session.session_start_time)
        self.assertEqual(stream.stream_end_time, session.session_end_time)

        # Verify stimulus epoch timing matches session timing
        epoch = session.stimulus_epochs[0]
        self.assertEqual(epoch.stimulus_start_time, session.session_start_time)
        self.assertEqual(epoch.stimulus_end_time, session.session_end_time)


if __name__ == "__main__":
    unittest.main()
