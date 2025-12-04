"""Tests opto and fiber benchmark session"""

import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from aind_metadata_mapper.opto_fiber_benchmark.models import JobSettings
from aind_metadata_mapper.opto_fiber_benchmark.session import (
    OptoFiberBenchmark,
)


class TestOptoFiberBenchmark(unittest.TestCase):
    """Class for running tests"""

    def setUp(self):
        """Create a temporary directory and mock JobSettings"""
        from tempfile import TemporaryDirectory

        self.temp_dir = TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)

        # Create dummy CSV files
        signal_file = self.data_dir / "Signal_2025-10-07T12_00_00.csv"
        stim_file = self.data_dir / "Stim_2025-10-07T12_00_00.csv"
        pd.DataFrame({"SoftwareTS": [0, 1, 2]}).to_csv(
            signal_file, index=False
        )
        pd.DataFrame({"SoftwareTS": [1, 2, 3]}).to_csv(stim_file, index=False)

        with patch("sys.argv", ["script_name"]):
            self.js = JobSettings.model_validate(
                {
                    "fiber": {
                        "data_directory": str(self.data_dir),
                        "data_streams": [
                            {
                                "light_sources": [
                                    {
                                        "name": "470nm LED",
                                        "excitation_power": 0.020,
                                        "excitation_power_unit": "milliwatt",
                                    }
                                ],
                                "detectors": [
                                    {
                                        "name": "Hamamatsu Camera",
                                        "exposure_time": 10,
                                        "trigger_type": "Internal",
                                    }
                                ],
                                "fiber_connections": [
                                    {
                                        "patch_cord_name": "Patch Cord A",
                                        "patch_cord_output_power": 40,
                                        "output_power_unit": "microwatt",
                                        "fiber_name": "Fiber A",
                                    }
                                ],
                            }
                        ],
                        "subject_id": "Mouse1",
                        "experimenter_full_name": ["Dr. Tester"],
                        "rig_id": "RigA",
                        "iacuc_protocol": "IACUC123",
                        "notes": "Test notes",
                        "mouse_platform_name": "PlatformA",
                        "active_mouse_platform": True,
                        "session_type": "OptoFiber",
                        "anaesthesia": "None",
                        "animal_weight_post": 25.0,
                        "animal_weight_prior": 24.5,
                    },
                    "opto": {
                        "data_directory": str(self.data_dir),
                        "pulse_frequency": [10.0],
                        "number_pulse_trains": [1],
                        "pulse_width": [5],
                        "pulse_train_duration": [1.0],
                        "fixed_pulse_train_interval": True,
                        "pulse_train_interval": 2.0,
                        "baseline_duration": 5.0,
                        "wavelength": 470,
                        "power": 10.0,
                        "laser_name": "Laser Stimulation",
                        "trials_total": 40,
                    },
                }
            )

    def tearDown(self):
        """Clean up after tests have run"""
        self.temp_dir.cleanup()

    def test_extract_session_start_time(self):
        """Tests extracting the session start time"""
        etl = OptoFiberBenchmark(self.js)
        data_files = list(
            Path(self.js.fiber.data_directory).glob("*Signal*.csv")
        )
        start_time = etl._extract_session_start_time(data_files)
        expected_time = datetime.strptime(
            "Signal_2025-10-07T12_00_00", "Signal_%Y-%m-%dT%H_%M_%S"
        )
        self.assertEqual(start_time, expected_time)

    def test_extract_stimulus_epochs(self):
        """Tests extracting stimulus epochs"""
        etl = OptoFiberBenchmark(self.js)
        data_files = list(
            Path(self.js.fiber.data_directory).glob("*Stim*.csv")
        )
        stim_info = etl._extract_stimulus_epochs(
            [Path("/dummy/Signal_2025-10-07T12_00_00.csv")] + data_files
        )
        self.assertEqual(stim_info["stimulus_name"], "OptoStim")
        self.assertEqual(stim_info["wavelength"], 470)
        self.assertEqual(stim_info["power"], 10.0)
        self.assertIsInstance(stim_info["stimulus_start_time"], str)
        self.assertIsInstance(stim_info["stimulus_end_time"], str)

    def test_extract_returns_model(self):
        """Tests returning intermediate model"""
        etl = OptoFiberBenchmark(self.js)
        model = etl._extract()
        from dataclasses import is_dataclass

        self.assertTrue(is_dataclass(model))
        self.assertTrue(hasattr(model, "fiber_data"))
        self.assertTrue(hasattr(model, "stimulus_epoch"))
        self.assertEqual(model.fiber_data.subject_id, "Mouse1")

    def test_transform_returns_session(self):
        """Tests returning session"""
        etl = OptoFiberBenchmark(self.js)
        model = etl._extract()
        session = etl._transfrom(model)
        self.assertEqual(session.subject_id, "Mouse1")
        self.assertEqual(session.session_type, "OptoFiber")
        self.assertEqual(len(session.data_streams), 1)
        self.assertEqual(len(session.stimulus_epochs), 1)

    def test_run_job_creates_file(self):
        """Tests running job"""
        etl = OptoFiberBenchmark(self.js)
        with patch(
            "aind_data_schema.core.session.Session.write_standard_file"
        ) as mock_write:
            response = etl.run_job()
            mock_write.assert_called_once()
            self.assertEqual(response.status_code, 200)
            self.assertIn("Wrote model to", response.message)


if __name__ == "__main__":
    unittest.main()
