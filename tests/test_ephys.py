"""Tests parsing of session information from ephys rig."""

import csv
import json
import os
import unittest
import zoneinfo
from pathlib import Path
from xml.dom import minidom

from aind_data_schema.core.session import Session

from aind_metadata_mapper.ephys.session import EphysEtl

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "resources" / "ephys"
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


class TestEphysSession(unittest.TestCase):
    """Test methods in ephys session module."""

    maxDiff = None  # show full diff without truncation

    @classmethod
    def setUpClass(cls):
        """Load record object and user settings before running tests."""
        # TODO: Add visual stimulus
        cls.experiment_data = {
            "experimenter_full_name": ["Al Dente"],
            "subject_id": "699889",
            "session_type": "Receptive field mapping",
            "iacuc_protocol": "2109",
            "rig_id": "323_EPHYS2-RF_2024-01-18_01",
            "animal_weight_prior": None,
            "animal_weight_post": None,
            "calibrations": [],
            "maintenance": [],
            "camera_names": [],
            "stick_microscopes": [
                {
                    "assembly_name": "20516338",
                    "arc_angle": -180.0,
                    "module_angle": -180.0,
                    "angle_unit": "degrees",
                    "notes": "Did not record arc or module angles, "
                    "did not calibrate",
                },
                {
                    "assembly_name": "22437106",
                    "arc_angle": -180.0,
                    "module_angle": -180.0,
                    "angle_unit": "degrees",
                    "notes": "Did not record arc or module angles, "
                    "did not calibrate",
                },
                {
                    "assembly_name": "22437107",
                    "arc_angle": -180.0,
                    "module_angle": -180.0,
                    "angle_unit": "degrees",
                    "notes": "Did not record arc or module angles, "
                    "did not calibrate",
                },
                {
                    "assembly_name": "22438379",
                    "arc_angle": -180.0,
                    "module_angle": -180.0,
                    "angle_unit": "degrees",
                    "notes": "Did not record arc or module angles, "
                    "did not calibrate",
                },
            ],
            "daqs": "Basestation",
            # data streams have to be in same
            # order as setting.xml's and newscale.csv's
            "data_streams": [
                {
                    "ephys_module_46121": {
                        "arc_angle": 5.3,
                        "module_angle": -27.1,
                        "angle_unit": "degrees",
                        "coordinate_transform": "behavior/"
                        "calibration_info_np2_2024_01_17T15_04_00.npy",
                        "calibration_date": "2024-01-17T15:04:00+00:00",
                        "notes": "Easy insertion. Recorded 8 minutes, "
                        "serially, so separate from prior insertion.",
                        "primary_targeted_structure": "AntComMid",
                        "targeted_ccf_coordinates": [
                            {
                                "ml": 5700.0,
                                "ap": 5160.0,
                                "dv": 5260.0,
                                "unit": "micrometer",
                                "ccf_version": "CCFv3",
                            }
                        ],
                    },
                    "ephys_module_46118": {
                        "arc_angle": 14,
                        "module_angle": 20,
                        "angle_unit": "degrees",
                        "coordinate_transform": "behavior/"
                        "calibration_info_np2_2024_01_17T15_04_00.npy",
                        "calibration_date": "2024-01-17T15:04:00+00:00",
                        "notes": "Easy insertion. Recorded 8 minutes, "
                        "serially, so separate from prior insertion.",
                        "primary_targeted_structure": "VISp",
                        "targeted_ccf_coordinates": [
                            {
                                "ml": 5700.0,
                                "ap": 5160.0,
                                "dv": 5260.0,
                                "unit": "micrometer",
                                "ccf_version": "CCFv3",
                            }
                        ],
                    },
                    "mouse_platform_name": "Running Wheel",
                    "active_mouse_platform": False,
                    "notes": "699889_2024-01-18_12-12-04",
                },
                {
                    "ephys_module_46121": {
                        "arc_angle": 5.3,
                        "module_angle": -27.1,
                        "angle_unit": "degrees",
                        "coordinate_transform": "behavior/"
                        "calibration_info_np2_2024_01_17T15_04_00.npy",
                        "calibration_date": "2024-01-17T15:04:00+00:00",
                        "notes": "Easy insertion. Recorded 8 minutes, "
                        "serially, so separate from prior insertion.",
                        "primary_targeted_structure": "AntComMid",
                        "targeted_ccf_coordinates": [
                            {
                                "ml": 5700.0,
                                "ap": 5160.0,
                                "dv": 5260.0,
                                "unit": "micrometer",
                                "ccf_version": "CCFv3",
                            }
                        ],
                    },
                    "ephys_module_46118": {
                        "arc_angle": 14,
                        "module_angle": 20,
                        "angle_unit": "degrees",
                        "coordinate_transform": "behavior/"
                        "calibration_info_np2_2024_01_17T15_04_00.npy",
                        "calibration_date": "2024-01-17T15:04:00+00:00",
                        "notes": "Easy insertion. Recorded 8 minutes, "
                        "serially, so separate from prior insertion.",
                        "primary_targeted_structure": "VISp",
                        "targeted_ccf_coordinates": [
                            {
                                "ml": 5700.0,
                                "ap": 5160.0,
                                "dv": 5260.0,
                                "unit": "micrometer",
                                "ccf_version": "CCFv3",
                            }
                        ],
                    },
                    "mouse_platform_name": "Running Wheel",
                    "active_mouse_platform": False,
                    "notes": "699889_2024-01-18_12-24-55; Surface Finding",
                },
            ],
        }

        stage_logs = []
        openephys_logs = []
        for stage, openephys in zip(
            EXAMPLE_STAGE_LOGS, EXAMPLE_OPENEPHYS_LOGS
        ):
            with open(stage, "r") as f:
                stage_logs.append([row for row in csv.reader(f)])
            with open(openephys, "r") as f:
                openephys_logs.append(minidom.parse(f))

        with open(EXPECTED_SESSION, "r") as f:
            expected_session = Session(**json.load(f))

        cls.stage_logs = stage_logs
        cls.openephys_logs = openephys_logs
        cls.expected_session = expected_session

    def test_extract(self):
        """Tests that the stage and openophys logs and experiment
        data is extracted correctly"""

        etl_job1 = EphysEtl(
            output_directory=RESOURCES_DIR,
            stage_logs=self.stage_logs,
            openephys_logs=self.openephys_logs,
            experiment_data=self.experiment_data,
        )
        parsed_info = etl_job1._extract()
        self.assertEqual(self.stage_logs, parsed_info.stage_logs)
        self.assertEqual(self.openephys_logs, parsed_info.openephys_logs)
        self.assertEqual(self.experiment_data, parsed_info.experiment_data)

    def test_transform(self):
        """Tests that the teensy response maps correctly to ophys session."""

        etl_job1 = EphysEtl(
            output_directory=RESOURCES_DIR,
            stage_logs=self.stage_logs,
            openephys_logs=self.openephys_logs,
            experiment_data=self.experiment_data,
        )
        parsed_info = etl_job1._extract()
        actual_session = etl_job1._transform(parsed_info)
        actual_session.session_start_time = (
            actual_session.session_start_time.replace(
                tzinfo=zoneinfo.ZoneInfo("UTC")
            )
        )
        actual_session.session_end_time = (
            actual_session.session_end_time.replace(
                tzinfo=zoneinfo.ZoneInfo("UTC")
            )
        )
        for stream in actual_session.data_streams:
            stream.stream_start_time = stream.stream_start_time.replace(
                tzinfo=zoneinfo.ZoneInfo("UTC")
            )
            stream.stream_end_time = stream.stream_end_time.replace(
                tzinfo=zoneinfo.ZoneInfo("UTC")
            )
        self.assertEqual(
            self.expected_session.model_dump(),
            actual_session.model_dump(),
        )


if __name__ == "__main__":
    unittest.main()
