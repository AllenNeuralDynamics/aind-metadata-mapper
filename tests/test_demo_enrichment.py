"""Tests for the enrichment demo CLI."""

import json
import runpy
import unittest
from pathlib import Path
from unittest.mock import patch

WEIGHT_RECORDS = [
    {"weight": 20.5, "weight_datetime": "2020-01-01T00:00:00Z"},
    {"weight": 21.5, "weight_datetime": "2030-01-01T00:00:00Z"},
]


class TestDemoEnrichment(unittest.TestCase):
    """Check the demo enriches both the built-in session and a supplied acquisition file."""

    def test_demo_prints_before_and_after(self):
        """Both inputs are enriched through the real enrichers, with the service calls mocked."""
        project_dir = Path(__file__).resolve().parents[1]
        script = project_dir / "scripts" / "demo_enrichment.py"
        acquisition_file = project_dir / "tests" / "resources" / "v2_metadata" / "acquisition.json"
        with acquisition_file.open() as resource:
            stored_acquisition = json.load(resource)
        for arguments, expected_subject, existing_ethics_review_id in (
            ([], "864846", None),
            (
                ["--acquisition-file", str(acquisition_file)],
                stored_acquisition["subject_id"],
                stored_acquisition["ethics_review_id"],
            ),
        ):
            with self.subTest(arguments=arguments):
                subject = {"subject_id": expected_subject, "subject_details": {"object_type": "Mouse subject"}}
                with (
                    patch("sys.argv", [str(script), *arguments]),
                    patch("aind_metadata_mapper.utils.get_subject", return_value=subject) as mock_get_subject,
                    patch("aind_metadata_mapper.enrichment.metadata_service_helper", return_value=WEIGHT_RECORDS),
                    patch("aind_metadata_mapper.enrichment.get_iacuc_protocol", return_value="2414"),
                    patch("builtins.print") as mock_print,
                ):
                    runpy.run_path(str(script), run_name="__main__")
                mock_get_subject.assert_called_once_with(
                    expected_subject, base_url="http://aind-metadata-service/api/v2/subject/"
                )
                output = json.loads(mock_print.call_args.args[0])
                self.assertEqual(output["subject_id"], expected_subject)
                self.assertIsNone(output["before"]["subject_details.animal_weight_prior"])
                self.assertIsNone(output["before"]["subject_details.animal_weight_post"])
                self.assertEqual(output["before"]["ethics_review_id"], existing_ethics_review_id)
                self.assertEqual(output["after"]["subject_details.animal_weight_prior"], "20.5")
                self.assertEqual(output["after"]["subject_details.animal_weight_post"], "21.5")
                self.assertEqual(output["after"]["ethics_review_id"], existing_ethics_review_id or ["2414"])
