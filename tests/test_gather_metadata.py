"""Tests gather_metadata module"""

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.core.metadata import Metadata
from aind_data_schema.core.processing import DataProcess, PipelineProcess
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.process_names import ProcessName
from requests import Response

from aind_metadata_mapper.gather_metadata import (
    GatherMetadataJob,
    JobSettings,
    MetadataSettings,
    ProceduresSettings,
    ProcessingSettings,
    RawDataDescriptionSettings,
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
        with open(RESOURCES_DIR / "example_funding_response.json", "r") as f:
            example_funding_response = json.load(f)
        with open(
            RESOURCES_DIR / "example_funding_multiple_response.json", "r"
        ) as f:
            example_funding_multi_response = json.load(f)
        cls.example_subject_response = example_subject_response
        cls.example_procedures_response = example_procedures_response
        cls.example_funding_response = example_funding_response
        cls.example_funding_multi_response = example_funding_multi_response

    def test_class_constructor(self):
        """Tests class is constructed properly"""
        job_settings = JobSettings(directory_to_write_to=RESOURCES_DIR)
        metadata_job = GatherMetadataJob(settings=job_settings)
        self.assertIsNotNone(metadata_job)

    @patch("pathlib.Path.is_file")
    def test_does_file_exist_in_user_defined_dir_path_true(
        self, mock_is_file: MagicMock
    ):
        """Tests _does_file_exist_in_user_defined_dir method returns true
        when path exists"""

        mock_is_file.return_value = True
        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR, metadata_dir="some_path"
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        self.assertTrue(
            metadata_job._does_file_exist_in_user_defined_dir("subject.json")
        )

    @patch("pathlib.Path.is_file")
    def test_does_file_exist_in_user_defined_dir_path_false(
        self, mock_is_file: MagicMock
    ):
        """Tests _does_file_exist_in_user_defined_dir method when path does
        not exist"""

        mock_is_file.return_value = False
        job_settings1 = JobSettings(directory_to_write_to=RESOURCES_DIR)
        metadata_job = GatherMetadataJob(settings=job_settings1)
        job_settings2 = JobSettings(
            directory_to_write_to=RESOURCES_DIR, metadata_dir="some_path"
        )
        metadata_job2 = GatherMetadataJob(settings=job_settings2)
        self.assertFalse(
            metadata_job._does_file_exist_in_user_defined_dir("subject.json")
        )
        self.assertFalse(
            metadata_job2._does_file_exist_in_user_defined_dir("subject.json")
        )

    def test_get_file_from_user_defined_directory(self):
        """Tests json contents are pulled correctly"""
        metadata_dir = RESOURCES_DIR / "metadata_files"
        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR, metadata_dir=metadata_dir
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job._get_file_from_user_defined_directory(
            "subject.json"
        )
        self.assertIsNotNone(contents)

    @patch("requests.get")
    def test_get_subject(self, mock_get: MagicMock):
        """Tests get_subject method when use service is true"""
        mock_response = Response()
        mock_response.status_code = 200
        body = json.dumps(self.example_subject_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            metadata_service_domain="http://acme.test",
            directory_to_write_to=RESOURCES_DIR,
            subject_settings=SubjectSettings(
                subject_id="632269",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_subject()
        self.assertEqual("632269", contents["subject_id"])
        mock_get.assert_called_once_with("http://acme.test/subject/632269")

    @patch("requests.get")
    def test_get_subject_from_dir(self, mock_get: MagicMock):
        """Tests get_subject method when use service is false"""
        mock_response = Response()
        mock_response.status_code = 200
        body = json.dumps(self.example_subject_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        metadata_dir = RESOURCES_DIR / "metadata_files"
        job_settings = JobSettings(
            metadata_service_domain="http://acme.test",
            directory_to_write_to=RESOURCES_DIR,
            subject_settings=SubjectSettings(
                subject_id="632269",
            ),
            metadata_dir=metadata_dir,
            metadata_dir_force=True,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_subject()
        self.assertEqual("632269", contents["subject_id"])
        mock_get.assert_not_called()

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
            metadata_service_domain="http://acme.test",
            subject_settings=SubjectSettings(
                subject_id="632269",
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
            metadata_service_domain="http://acme.test",
            procedures_settings=ProceduresSettings(
                subject_id="632269",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_procedures()
        self.assertEqual("632269", contents["subject_id"])
        mock_get.assert_called_once_with("http://acme.test/procedures/632269")

    @patch("requests.get")
    def test_get_procedures_from_dir(self, mock_get: MagicMock):
        """Tests get_procedures method from dir"""
        mock_response = Response()
        mock_response.status_code = 406
        body = json.dumps(self.example_procedures_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response
        metadata_dir = RESOURCES_DIR / "metadata_files"
        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_service_domain="http://acme.test",
            procedures_settings=ProceduresSettings(
                subject_id="632269",
            ),
            metadata_dir=metadata_dir,
            metadata_dir_force=True,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_procedures()
        self.assertEqual("632269", contents["subject_id"])
        mock_get.assert_not_called()

    @patch("requests.get")
    @patch("logging.warning")
    def test_get_procedures_warning(
        self, mock_warn: MagicMock, mock_get: MagicMock
    ):
        """Tests get_procedures when an error is raised"""
        mock_response = Response()
        mock_response.status_code = 500
        body = json.dumps({"message": "Internal Server Error"})
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_service_domain="http://acme.test",
            procedures_settings=ProceduresSettings(
                subject_id="632269",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        output = metadata_job.get_procedures()
        self.assertIsNone(output)
        mock_warn.assert_called_once_with(
            "Procedures metadata is not valid! 500"
        )

    @patch("requests.get")
    def test_get_raw_data_description(self, mock_get: MagicMock):
        """Tests get_raw_data_description method with valid model"""

        mock_response = Response()
        mock_response.status_code = 200
        body = json.dumps(self.example_funding_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_service_domain="http://acme.test",
            raw_data_description_settings=RawDataDescriptionSettings(
                project_name="Ephys Platform",
                name="ecephys_632269_2023-10-10_10-10-10",
                modality=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_raw_data_description()
        expected_investigators = ["Anna Apple", "John Smith"]
        actual_investigators = [i["name"] for i in contents["investigators"]]
        self.assertEqual(expected_investigators, actual_investigators)
        self.assertEqual("ecephys", contents["platform"]["abbreviation"])
        self.assertEqual("632269", contents["subject_id"])
        self.assertEqual(
            "ecephys_632269_2023-10-10_10-10-10", contents["name"]
        )
        mock_get.assert_called_once_with(
            "http://acme.test/funding/Ephys Platform"
        )

    @patch("requests.get")
    def test_get_raw_data_description_from_dir(self, mock_get: MagicMock):
        """Tests get_raw_data_description method from dir"""

        mock_response = Response()
        mock_response.status_code = 200
        body = json.dumps(self.example_funding_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        metadata_dir = RESOURCES_DIR / "metadata_files"

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_service_domain="http://acme.test",
            raw_data_description_settings=RawDataDescriptionSettings(
                project_name="Ephys Platform",
                name="ecephys_632269_2023-10-10_10-10-10",
                modality=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
            ),
            metadata_dir=metadata_dir,
            metadata_dir_force=True,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_raw_data_description()
        expected_investigators = ["Anna Apple", "John Smith"]
        actual_investigators = [i["name"] for i in contents["investigators"]]
        self.assertEqual(expected_investigators, actual_investigators)
        self.assertEqual("ecephys", contents["platform"]["abbreviation"])
        self.assertEqual("632269", contents["subject_id"])
        self.assertEqual(
            "ecephys_632269_2023-10-10_10-10-10", contents["name"]
        )
        mock_get.assert_not_called()

    @patch("requests.get")
    def test_get_raw_data_description_multi_response(
        self, mock_get: MagicMock
    ):
        """Tests get_raw_data_description method with valid model and multiple
        items in funding response"""

        mock_response = Response()
        mock_response.status_code = 300
        body = json.dumps(self.example_funding_multi_response)
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_service_domain="http://acme.test",
            raw_data_description_settings=RawDataDescriptionSettings(
                project_name="Ephys Platform",
                name="ecephys_632269_2023-10-10_10-10-10",
                modality=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_raw_data_description()
        expected_investigators = ["Anna Apple", "Don Key", "John Smith"]
        actual_investigators = [i["name"] for i in contents["investigators"]]
        self.assertEqual(2, len(contents["funding_source"]))
        self.assertEqual(expected_investigators, actual_investigators)
        self.assertEqual("ecephys", contents["platform"]["abbreviation"])
        self.assertEqual("632269", contents["subject_id"])
        self.assertEqual(
            "ecephys_632269_2023-10-10_10-10-10", contents["name"]
        )
        mock_get.assert_called_once_with(
            "http://acme.test/funding/Ephys Platform"
        )

    @patch("requests.get")
    def test_get_raw_data_description_invalid(self, mock_get: MagicMock):
        """Tests get_raw_data_description method with invalid model"""
        mock_response = Response()
        mock_response.status_code = 500
        body = json.dumps({"message": "Internal Server Error"})
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_service_domain="http://acme.test",
            raw_data_description_settings=RawDataDescriptionSettings(
                project_name="foo",
                name="ecephys_632269_2023-10-10_10-10-10",
                modality=[Modality.ECEPHYS, Modality.BEHAVIOR_VIDEOS],
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        # with self.assertWarns(UserWarning) as w:
        contents = metadata_job.get_raw_data_description()
        self.assertEqual("ecephys", contents["platform"]["abbreviation"])
        self.assertEqual("632269", contents["subject_id"])
        self.assertEqual(
            "ecephys_632269_2023-10-10_10-10-10", contents["name"]
        )
        self.assertEqual([], contents["investigators"])

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
            input_location="/source/open_ephys",
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

    def test_get_processing_metadata_from_dir(self):
        """Tests get_processing_metadata method from dir"""
        data_process = DataProcess(
            name=ProcessName.COMPRESSION,
            software_version="0.0.15",
            start_date_time=datetime(
                2020, 10, 10, 10, 10, 10, tzinfo=timezone.utc
            ),
            end_date_time=datetime(
                2020, 10, 10, 11, 10, 10, tzinfo=timezone.utc
            ),
            input_location="/source/open_ephys",
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

        metadata_dir = RESOURCES_DIR / "metadata_files"

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            processing_settings=ProcessingSettings(
                pipeline_process=processing_pipeline
            ),
            metadata_dir=metadata_dir,
            metadata_dir_force=True,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_processing_metadata()
        self.assertEqual(
            "Compression",
            contents["processing_pipeline"]["data_processes"][0]["name"],
        )

    def test_get_session_metadata(self):
        """Tests get_session_metadata"""
        metadata_dir = RESOURCES_DIR / "metadata_files"

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_dir=metadata_dir,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_session_metadata()
        self.assertIsNotNone(contents)

    def test_get_session_metadata_none(self):
        """Tests get_session_metadata returns none"""

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_session_metadata()
        self.assertIsNone(contents)

    def test_get_rig_metadata(self):
        """Tests get_rig_metadata"""
        metadata_dir = RESOURCES_DIR / "metadata_files"

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_dir=metadata_dir,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_rig_metadata()
        self.assertIsNotNone(contents)

    def test_get_rig_metadata_none(self):
        """Tests get_rig_metadata returns none"""

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_rig_metadata()
        self.assertIsNone(contents)

    def test_get_acquisition_metadata(self):
        """Tests get_acquisition_metadata"""
        metadata_dir = RESOURCES_DIR / "metadata_files"

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_dir=metadata_dir,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_acquisition_metadata()
        self.assertIsNotNone(contents)

    def test_get_acquisition_metadata_none(self):
        """Tests get_acquisition_metadata returns none"""

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_acquisition_metadata()
        self.assertIsNone(contents)

    def test_get_instrument_metadata(self):
        """Tests get_instrument_metadata"""
        metadata_dir = RESOURCES_DIR / "metadata_files"

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_dir=metadata_dir,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_instrument_metadata()
        self.assertIsNotNone(contents)

    def test_get_instrument_metadata_none(self):
        """Tests get_instrument_metadata returns none"""

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_instrument_metadata()
        self.assertIsNone(contents)

    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob."
        "_write_json_file"
    )
    def test_gather_non_automated_metadata(self, mock_write_file: MagicMock):
        """Tests _gather_non_automated_metadata method"""
        metadata_dir = RESOURCES_DIR / "metadata_files"

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_dir=metadata_dir,
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        metadata_job._gather_non_automated_metadata()
        mock_write_file.assert_called()

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
            "  Expected `date` but got `str`"
            " - serialized value may not be as expected\n"
            "  Expected `Union[CallithrixJacchus, HomoSapiens, MacacaMulatta, "
            "MusMusculus, RattusNorvegicus]` but got `dict`"
            " - serialized value may not be as expected\n"
            "  Expected `BreedingInfo` but got `dict`"
            " - serialized value may not be as expected\n"
            "  Expected `Union[AllenInstitute, ColumbiaUniversity,"
            " HuazhongUniversityOfScienceAndTechnology, JaneliaResearchCampus,"
            " JacksonLaboratory, NewYorkUniversity, Other]` but got `dict`"
            " - serialized value may not be as expected"
        )
        self.assertEqual(expected_warnings, str(w.warning))
        self.assertEqual(
            "s3://some-bucket/ecephys_632269_2023-10-10_10-10-10",
            main_metadata.location,
        )
        self.assertEqual("Invalid", main_metadata.metadata_status.value)
        self.assertEqual("632269", main_metadata.subject.subject_id)

    @patch("logging.warning")
    def test_get_main_metadata_with_validation_errors(
        self, mock_warn: MagicMock
    ):
        """Tests get_main_metadata method handles validation errors"""
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
                session_filepath=(METADATA_DIR / "session.json"),
                rig_filepath=(METADATA_DIR / "rig.json"),
                processing_filepath=(METADATA_DIR / "processing.json"),
                acquisition_filepath=(METADATA_DIR / "acquisition.json"),
                instrument_filepath=(METADATA_DIR / "instrument.json"),
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        main_metadata = metadata_job.get_main_metadata()
        self.assertIsNotNone(main_metadata.subject)
        self.assertIsNotNone(main_metadata.procedures)
        self.assertIsNotNone(main_metadata.data_description)
        self.assertIsNotNone(main_metadata.session)
        self.assertIsNotNone(main_metadata.rig)
        self.assertIsNotNone(main_metadata.processing)
        self.assertIsNotNone(main_metadata.acquisition)
        self.assertIsNotNone(main_metadata.instrument)
        mock_warn.assert_called_once()

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
    @patch("builtins.open", new_callable=mock_open())
    @patch("json.dump")
    def test_run_job(
        self,
        mock_json_dump: MagicMock,
        mock_open_file: MagicMock,
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
            input_location="/source/open_ephys",
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
            metadata_service_domain="http://acme.test",
            subject_settings=SubjectSettings(
                subject_id="632269",
            ),
            procedures_settings=ProceduresSettings(
                subject_id="632269",
            ),
            raw_data_description_settings=RawDataDescriptionSettings(
                project_name="Ephys Platform",
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

        # TODO: Add better mocked response
        mock_get_main_metadata.return_value = Metadata.model_construct()

        metadata_job = GatherMetadataJob(settings=job_settings)

        metadata_job.run_job()

        mock_get_subject.assert_called_once()
        mock_get_procedures.assert_called_once()
        mock_get_raw_data_description.assert_called_once()
        mock_get_processing_metadata.assert_called_once()
        mock_get_main_metadata.assert_called_once()
        mock_write_json_file.assert_called()
        mock_open_file.assert_called()
        mock_json_dump.assert_called()

    @patch("builtins.open", new_callable=mock_open())
    @patch("json.dump")
    def test_run_job_main_metadata(
        self, mock_json_dump: MagicMock, mock_write_file: MagicMock
    ):
        """Tests run job writes metadata json correctly"""

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_settings=MetadataSettings(
                name="ecephys_632269_2023-10-10_10-10-10",
                location="s3://some-bucket/ecephys_632269_2023-10-10_10-10-10",
            ),
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        metadata_job.run_job()
        json_contents = mock_json_dump.mock_calls[0].args[0]
        mock_write_file.assert_called()
        self.assertIsNotNone(json_contents.get("_id"))
        self.assertIsNone(json_contents.get("id"))


if __name__ == "__main__":
    unittest.main()
