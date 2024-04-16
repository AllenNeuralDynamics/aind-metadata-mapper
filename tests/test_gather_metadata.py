"""Tests gather_metadata module"""

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.core.processing import DataProcess, PipelineProcess
from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.pid_names import PIDName
from aind_data_schema.models.process_names import ProcessName
from requests import Response

from aind_metadata_mapper.gather_metadata import (
    DataDescriptionSettings,
    GatherMetadataJob,
    JobSettings,
    MetadataSettings,
    ProceduresSettings,
    ProcessingSettings,
    SubjectSettings,
)

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / "resources"
    / "gather_metadata_job"
)
METADATA_DIR = RESOURCES_DIR / "metadata_files"


class TestGatherMetadataJob(unittest.TestCase):
    """Tests methods in GatherMetadataJob class"""

    @classmethod
    def setUpClass(cls):
        """Load json files."""
        with open(RESOURCES_DIR / "example_subject_response.json", "r") as f:
            example_subject_response = json.load(f)
        with open(
            RESOURCES_DIR / "example_procedures_response.json", "r"
        ) as f:
            example_procedures_response = json.load(f)
        cls.example_subject_response = example_subject_response
        cls.example_procedures_response = example_procedures_response

    def test_class_constructor(self):
        """Tests class is constructed properly"""
        job_settings = JobSettings(directory_to_write_to=RESOURCES_DIR)
        metadata_job = GatherMetadataJob(settings=job_settings)
        self.assertIsNotNone(metadata_job)

    @patch("requests.get")
    def test_get_subject(self, mock_get: MagicMock):
        """Tests get_subject method"""
        mock_response = Response()
        mock_response.status_code = 200
        body = json.dumps(self.example_subject_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            subject_settings=SubjectSettings(
                subject_id="632269",
                metadata_service_url="http://acme.test",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_subject()
        self.assertEqual("632269", contents["subject_id"])

    @patch("requests.get")
    def test_get_subject_error(self, mock_get: MagicMock):
        """Tests get_subject when an error is raised"""
        mock_response = Response()
        mock_response.status_code = 500
        body = json.dumps({"message": "Internal Server Error"})
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            subject_settings=SubjectSettings(
                subject_id="632269",
                metadata_service_url="http://acme.test",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        with self.assertRaises(AssertionError) as e:
            metadata_job.get_subject()
        expected_error_message = (
            "Subject metadata is not valid! "
            "{'message': 'Internal Server Error'}"
        )
        self.assertTrue(expected_error_message in str(e.exception))

    @patch("requests.get")
    def test_get_procedures(self, mock_get: MagicMock):
        """Tests get_procedures method"""
        mock_response = Response()
        mock_response.status_code = 406
        body = json.dumps(self.example_procedures_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            procedures_settings=ProceduresSettings(
                subject_id="632269",
                metadata_service_url="http://acme.test",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_procedures()
        self.assertEqual("632269", contents["subject_id"])

    @patch("requests.get")
    def test_get_procedures_error(self, mock_get: MagicMock):
        """Tests get_procedures when an error is raised"""
        mock_response = Response()
        mock_response.status_code = 500
        body = json.dumps({"message": "Internal Server Error"})
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            procedures_settings=ProceduresSettings(
                subject_id="632269",
                metadata_service_url="http://acme.test",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        with self.assertRaises(AssertionError) as e:
            metadata_job.get_procedures()
        expected_error_message = (
            "Procedures metadata is not valid! "
            "{'message': 'Internal Server Error'}"
        )
        self.assertTrue(expected_error_message in str(e.exception))

    def test_get_raw_data_description(self):
        """Tests get_raw_data_description method with valid model"""
        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            data_description_settings=DataDescriptionSettings(
                investigators=[
                    PIDName(name="Anna Apple"),
                    PIDName(name="John Smith"),
                ],
                name="ecephys_632269_2023-10-10_10-10-10",
                modality=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_raw_data_description()
        self.assertEqual("ecephys", contents["platform"]["abbreviation"])
        self.assertEqual("632269", contents["subject_id"])
        self.assertEqual(
            "ecephys_632269_2023-10-10_10-10-10", contents["name"]
        )

    def test_get_raw_data_description_invalid(self):
        """Tests get_raw_data_description method with invalid model"""
        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            data_description_settings=DataDescriptionSettings(
                investigators=[],
                name="ecephys_632269_2023-10-10_10-10-10",
                modality=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        with self.assertWarns(UserWarning) as w:
            contents = metadata_job.get_raw_data_description()
        self.assertEqual("ecephys", contents["platform"]["abbreviation"])
        self.assertEqual("632269", contents["subject_id"])
        self.assertEqual(
            "ecephys_632269_2023-10-10_10-10-10", contents["name"]
        )
        expected_warning = (
            "Pydantic serializer warnings:\n"
            "  Expected `list[Funding]` but got `tuple` -"
            " serialized value may not be as expected"
        )
        self.assertEqual(expected_warning, str(w.warning))

    def test_get_processing_metadata(self):
        """Tests get_processing_metadata method"""
        data_process = DataProcess(
            name=ProcessName.COMPRESSION,
            software_version="0.0.15",
            start_date_time=datetime(
                2020, 10, 10, 10, 10, 10, tzinfo=timezone.utc
            ),
            end_date_time=datetime(
                2020, 10, 10, 11, 10, 10, tzinfo=timezone.utc
            ),
            input_location="/source/ephys",
            output_location="/tmp/stage",
            code_url=(
                "https://github.com/AllenNeuralDynamics/"
                "aind-data-transformation"
            ),
            parameters={},
            outputs={},
        )
        processing_pipeline = PipelineProcess(
            data_processes=[data_process], processor_full_name="Anna Apple"
        )

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            processing_settings=ProcessingSettings(
                pipeline_process=processing_pipeline
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_processing_metadata()
        self.assertEqual(
            "Compression",
            contents["processing_pipeline"]["data_processes"][0]["name"],
        )

    def test_get_main_metadata_with_warnings(self):
        """Tests get_main_metadata method raises validation warnings"""
        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_settings=MetadataSettings(
                name="ecephys_632269_2023-10-10_10-10-10",
                location="s3://some-bucket/ecephys_632269_2023-10-10_10-10-10",
                subject_filepath=(METADATA_DIR / "subject.json"),
                data_description_filepath=(
                    METADATA_DIR / "data_description.json"
                ),
                procedures_filepath=(METADATA_DIR / "procedures.json"),
                session_filepath=None,
                rig_filepath=None,
                processing_filepath=(METADATA_DIR / "processing.json"),
                acquisition_filepath=None,
                instrument_filepath=None,
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        with self.assertWarns(UserWarning) as w:
            main_metadata = metadata_job.get_main_metadata()
        # Issues with incomplete Procedures model raises warnings
        expected_warnings = (
            "Pydantic serializer warnings:\n"
            "  Expected `Union[Surgery, TrainingProtocol, WaterRestriction,"
            " definition-ref]` but got `dict` -"
            " serialized value may not be as expected\n"
            "  Expected `Union[Surgery, TrainingProtocol, WaterRestriction,"
            " definition-ref]` but got `dict` -"
            " serialized value may not be as expected"
        )
        self.assertEqual(expected_warnings, str(w.warning))

        self.assertEqual(
            "s3://some-bucket/ecephys_632269_2023-10-10_10-10-10",
            main_metadata.location,
        )
        self.assertEqual("Invalid", main_metadata.metadata_status.value)
        self.assertEqual("632269", main_metadata.subject.subject_id)

    @patch("builtins.open", new_callable=mock_open())
    @patch("json.dump")
    def test_write_json_file(
        self, mock_json_dump: MagicMock, mock_file: MagicMock
    ):
        """Tests write_json_file method"""
        mock_file.return_value.__enter__.return_value = (
            RESOURCES_DIR / "subject.json"
        )

        job_settings = JobSettings(directory_to_write_to=RESOURCES_DIR)
        metadata_job = GatherMetadataJob(settings=job_settings)
        metadata_job._write_json_file(
            filename="subject.json", contents={"subject_id": "123456"}
        )

        mock_json_dump.assert_called_once_with(
            {"subject_id": "123456"}, RESOURCES_DIR / "subject.json", indent=3
        )

    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob.get_subject"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob.get_procedures"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        ".get_raw_data_description"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        ".get_processing_metadata"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        ".get_main_metadata"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._write_json_file"
    )
    def test_run_job(
        self,
        mock_write_json_file: MagicMock,
        mock_get_main_metadata: MagicMock,
        mock_get_processing_metadata: MagicMock,
        mock_get_raw_data_description: MagicMock,
        mock_get_procedures: MagicMock,
        mock_get_subject: MagicMock,
    ):
        """Tests run_job calls all the sub processes"""
        data_process = DataProcess(
            name=ProcessName.COMPRESSION,
            software_version="0.0.15",
            start_date_time=datetime(
                2020, 10, 10, 10, 10, 10, tzinfo=timezone.utc
            ),
            end_date_time=datetime(
                2020, 10, 10, 11, 10, 10, tzinfo=timezone.utc
            ),
            input_location="/source/ephys",
            output_location="/tmp/stage",
            code_url=(
                "https://github.com/AllenNeuralDynamics/"
                "aind-data-transformation"
            ),
            parameters={},
            outputs={},
        )
        processing_pipeline = PipelineProcess(
            data_processes=[data_process], processor_full_name="Anna Apple"
        )

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            subject_settings=SubjectSettings(
                subject_id="632269",
                metadata_service_url="http://acme.test",
            ),
            procedures_settings=ProceduresSettings(
                subject_id="632269",
                metadata_service_url="http://acme.test",
            ),
            data_description_settings=DataDescriptionSettings(
                investigators=[
                    PIDName(name="Anna Apple"),
                    PIDName(name="John Smith"),
                ],
                name="ecephys_632269_2023-10-10_10-10-10",
                modality=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
            ),
            processing_settings=ProcessingSettings(
                pipeline_process=processing_pipeline
            ),
            metadata_settings=MetadataSettings(
                name="ecephys_632269_2023-10-10_10-10-10",
                location="s3://some-bucket/ecephys_632269_2023-10-10_10-10-10",
                subject_filepath=(METADATA_DIR / "subject.json"),
                data_description_filepath=(
                    METADATA_DIR / "data_description.json"
                ),
                procedures_filepath=(METADATA_DIR / "procedures.json"),
                session_filepath=None,
                rig_filepath=None,
                processing_filepath=(METADATA_DIR / "processing.json"),
                acquisition_filepath=None,
                instrument_filepath=None,
            ),
        )

        metadata_job = GatherMetadataJob(settings=job_settings)

        metadata_job.run_job()

        mock_get_subject.assert_called_once()
        mock_get_procedures.assert_called_once()
        mock_get_raw_data_description.assert_called_once()
        mock_get_processing_metadata.assert_called_once()
        mock_get_main_metadata.assert_called_once()
        mock_write_json_file.assert_called()


if __name__ == "__main__":
    unittest.main()
