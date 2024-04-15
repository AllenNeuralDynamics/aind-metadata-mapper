"""Tests gather_metadata module"""

import csv
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from aind_data_schema.core.session import Session
from requests import Response

from aind_metadata_mapper.gather_metadata import GatherMetadataJob, \
    JobSettings, SubjectSettings

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "resources" / "gather_metadata_job"
)


class TestGatherMetadataJob(unittest.TestCase):

    def test_class_constructor(self):
        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        self.assertIsNotNone(metadata_job)

    @patch("requests.get")
    def test_get_subject(self, mock_get: MagicMock):

        mock_response = Response()
        mock_response.status_code = 200
        body = (
            '{"message":"Valid Model.",'
            '"data":'
            '{"describedBy":"https://raw.githubusercontent.com/'
            'AllenNeuralDynamics/aind-data-schema/main/src/'
            'aind_data_schema/core/subject.py",'
            '"schema_version":"0.5.5",'
            '"subject_id":"632269",'
            '"sex":"Female",'
            '"date_of_birth":"2022-05-01",'
            '"genotype":"Pvalb-IRES-Cre/wt;RCL-somBiPoles_mCerulean-WPRE/wt",'
            '"species":{'
            '"name":"Mus musculus",'
            '"abbreviation":null,'
            '"registry":{'
            '"name":"National Center for Biotechnology Information",'
            '"abbreviation":"NCBI"},'
            '"registry_identifier":"10090"},'
            '"alleles":[],'
            '"background_strain":null,'
            '"breeding_info":{'
            '"breeding_group":'
            '"Pvalb-IRES-Cre;RCL-somBiPoles_mCerulean-WPRE(ND)",'
            '"maternal_id":"615310",'
            '"maternal_genotype":"Pvalb-IRES-Cre/wt",'
            '"paternal_id":"623236",'
            '"paternal_genotype":'
            '"RCL-somBiPoles_mCerulean-WPRE/wt"},'
            '"source":{'
            '"name":"Allen Institute",'
            '"abbreviation":"AI",'
            '"registry":{'
            '"name":"Research Organization Registry",'
            '"abbreviation":"ROR"},'
            '"registry_identifier":"03cpe7c52"},'
            '"rrid":null,'
            '"restrictions":null,'
            '"wellness_reports":[],'
            '"housing":null,'
            '"notes":null}}'
        )
        mock_response._content = body.encode("utf-8")
        mock_get.return_value = mock_response

        job_settings = JobSettings(
            directory_to_write_to=RESOURCES_DIR,
            metadata_service_url="http://acme.test",
            subject_settings=SubjectSettings(subject_id="632269")
        )
        metadata_job = GatherMetadataJob(settings=job_settings)
        contents = metadata_job.get_subject()
        self.assertEqual("632269", contents["subject_id"])


if __name__ == "__main__":
    unittest.main()
