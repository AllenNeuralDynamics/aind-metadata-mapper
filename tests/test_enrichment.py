"""Tests for the acquisition enrichment module."""

import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aind_data_schema.core.acquisition import Acquisition, AcquisitionSubjectDetails
from pydantic import ValidationError

from aind_metadata_mapper.enrichment import (
    add_ethics_review_id,
    add_subject_weights,
    enrich_acquisition,
    validate_acquisition,
    write_acquisition,
)
from aind_metadata_mapper.models import EnrichmentSettings

SERVICE_URL = "http://test-service.com"
SUBJECT_ID = "123456"
WEIGHT_RECORDS_URL = (
    "http://test-service.com/api/v2/dataverse/mouse_weight_records/123456?acquisition_datetime=2023-01-01T12%3A00%3A00"
)
LABTRACKS_URL = "http://test-service.com/api/v2/labtracks/subject"
V2_METADATA_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources" / "v2_metadata"
FIXTURE_SUBJECT_ID = "804670"
FIXTURE_WEIGHT_RECORDS_URL = (
    "http://test-service.com/api/v2/dataverse/mouse_weight_records/804670?acquisition_datetime=2025-09-17T10%3A26%3A00"
)
FIXTURE_WEIGHT_RECORDS = [
    {"weight": 20.5, "weight_datetime": "2025-09-17T17:00:00Z"},
    {"weight": 21.5, "weight_datetime": "2025-09-17T18:45:00Z"},
]


def mouse_subject() -> dict:
    """Return a minimal Mouse subject."""
    return {"subject_id": SUBJECT_ID, "subject_details": {"object_type": "Mouse subject"}}


def mouse_acquisition(**subject_details) -> dict:
    """Return a minimal acquisition for a wheel session starting at noon UTC. Not a valid Acquisition."""
    return {
        "acquisition_start_time": "2023-01-01T12:00:00+00:00",
        "acquisition_end_time": "2023-01-01T13:00:00+00:00",
        "subject_details": {"mouse_platform_name": "wheel", **subject_details},
    }


def fixture_acquisition() -> dict:
    """Return the valid v2 acquisition fixture with ethics_review_id removed so enrichment has to fill it."""
    with (V2_METADATA_DIR / "acquisition.json").open() as resource:
        acquisition = json.load(resource)
    del acquisition["ethics_review_id"]
    return acquisition


def fixture_subject() -> dict:
    """Return the v2 subject fixture, a Mouse subject."""
    with (V2_METADATA_DIR / "subject.json").open() as resource:
        return json.load(resource)


class TestAddSubjectWeights(unittest.TestCase):
    """Tests add_subject_weights"""

    def enrich(self, acquisition, raise_if_invalid=False, replace_existing=False):
        """Run add_subject_weights against the test service for a mouse subject."""
        return add_subject_weights(
            acquisition=acquisition,
            subject=mouse_subject(),
            subject_id=SUBJECT_ID,
            metadata_service_url=SERVICE_URL,
            raise_if_invalid=raise_if_invalid,
            replace_existing=replace_existing,
        )

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_uses_nearest_captures_on_each_side_of_start(self, mock_metadata_service_helper):
        """Test that captures after acquisition start are post weights."""
        acquisition = mouse_acquisition()
        mock_metadata_service_helper.return_value = [
            {"weight": 20.0, "weight_datetime": "2023-01-01T10:00:00Z"},
            {"weight": 21.0, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 22.0, "weight_datetime": "2023-01-01T12:30:00Z"},
            {"weight": 23.0, "weight_datetime": "2023-01-01T13:00:00Z"},
            {"weight": 24.0, "weight_datetime": "2023-01-01T13:00:01Z"},
            {"weight": 25.0, "weight_datetime": "2023-01-01T14:00:00Z"},
        ]

        result = self.enrich(acquisition)

        self.assertIs(result, acquisition)
        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 21)
        self.assertEqual(details.animal_weight_post, 22)
        self.assertEqual(acquisition["subject_details"], details.model_dump(mode="json"))
        mock_metadata_service_helper.assert_called_once_with(WEIGHT_RECORDS_URL)

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper", return_value=[])
    def test_without_matching_captures_warns_and_does_not_add_weights(self, mock_metadata_service_helper):
        """Test that no available captures leave pre and post weights unset, with a warning."""
        acquisition = mouse_acquisition()

        with self.assertLogs(level="WARNING") as captured:
            result = self.enrich(acquisition)

        self.assertIs(result, acquisition)
        self.assertEqual(acquisition["subject_details"], {"mouse_platform_name": "wheel"})
        self.assertIn("No Waterlog weight records", captured.output[0])
        mock_metadata_service_helper.assert_called_once()

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_without_acquisition_end_uses_start(self, mock_metadata_service_helper):
        """Test that acquisition end is unnecessary and exact start matches are pre weights."""
        acquisition = mouse_acquisition()
        del acquisition["acquisition_end_time"]
        mock_metadata_service_helper.return_value = [
            {"weight": 24.0, "weight_datetime": "2023-01-01T12:30:00Z"},
            {"weight": 22.0, "weight_datetime": "2023-01-01T07:00:00-05:00"},
            {"weight": 21.0, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 23.0, "weight_datetime": "2023-01-01T12:00:01Z"},
        ]

        self.enrich(acquisition)

        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 22)
        self.assertEqual(details.animal_weight_post, 23)
        mock_metadata_service_helper.assert_called_once()

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_without_acquisition_start_does_not_query(self, mock_metadata_service_helper):
        """Test that an acquisition without a start time is ignored."""
        acquisition = {"acquisition_end_time": "2023-01-01T13:00:00+00:00"}

        result = self.enrich(acquisition)

        self.assertIs(result, acquisition)
        self.assertEqual(acquisition, {"acquisition_end_time": "2023-01-01T13:00:00+00:00"})
        mock_metadata_service_helper.assert_not_called()

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_ignores_invalid_service_records(self, mock_metadata_service_helper):
        """Test that malformed weight records are skipped."""
        acquisition = mouse_acquisition()
        mock_metadata_service_helper.return_value = [
            None,
            {},
            {"weight": 22.0, "weight_datetime": "not-a-datetime"},
        ]

        self.enrich(acquisition)

        self.assertEqual(acquisition["subject_details"], {"mouse_platform_name": "wheel"})
        mock_metadata_service_helper.assert_called_once()

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper", return_value={"records": []})
    def test_ignores_non_list_service_response(self, mock_metadata_service_helper):
        """Test that a non-list service response leaves subject weights unchanged."""
        acquisition = mouse_acquisition()

        self.enrich(acquisition)

        self.assertEqual(acquisition["subject_details"], {"mouse_platform_name": "wheel"})
        mock_metadata_service_helper.assert_called_once()

    @patch("logging.warning")
    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_preserves_existing_values_and_warns_on_mismatch(self, mock_metadata_service_helper, mock_warning):
        """Test that existing weights are preserved and mismatches are warned about."""
        acquisition = mouse_acquisition(animal_weight_prior=19.0, animal_weight_post=25.0)
        mock_metadata_service_helper.return_value = [
            {"weight": 21.0, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 24.0, "weight_datetime": "2023-01-01T13:00:01Z"},
        ]

        self.enrich(acquisition)

        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 19)
        self.assertEqual(details.animal_weight_post, 25)
        self.assertEqual(mock_warning.call_count, 2)
        self.assertIn("animal_weight_prior", mock_warning.call_args_list[0].args[0])
        self.assertIn("animal_weight_post", mock_warning.call_args_list[1].args[0])

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_replace_existing_overwrites_existing_values(self, mock_metadata_service_helper):
        """Test that replace_existing=True overwrites existing weights with the fetched values."""
        acquisition = mouse_acquisition(animal_weight_prior=19.0, animal_weight_post=25.0)
        mock_metadata_service_helper.return_value = [
            {"weight": 21.0, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 24.0, "weight_datetime": "2023-01-01T13:00:01Z"},
        ]

        self.enrich(acquisition, replace_existing=True)

        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 21)
        self.assertEqual(details.animal_weight_post, 24)

    @patch("logging.warning")
    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_does_not_warn_when_existing_values_match(self, mock_metadata_service_helper, mock_warning):
        """Test that matching existing weights are retained without warnings."""
        acquisition = mouse_acquisition(animal_weight_prior="21.1", animal_weight_post="24.1")
        mock_metadata_service_helper.return_value = [
            {"weight": 21.1, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 24.1, "weight_datetime": "2023-01-01T13:00:01Z"},
        ]

        self.enrich(acquisition)

        self.assertEqual(acquisition["subject_details"]["animal_weight_prior"], "21.1")
        self.assertEqual(acquisition["subject_details"]["animal_weight_post"], "24.1")
        mock_warning.assert_not_called()

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_invalid_details_respect_raise_if_invalid(self, mock_metadata_service_helper):
        """Invalid modified acquisition details must not be saved."""
        mock_metadata_service_helper.return_value = [
            {"weight": 21, "weight_datetime": "2023-01-01T12:00:00Z"},
        ]
        for details in (None, {}, {"mouse_platform_name": "wheel", "pre_weight": 20}):
            for strict in (False, True):
                with self.subTest(details=details, strict=strict):
                    acquisition = {
                        "acquisition_start_time": "2023-01-01T12:00:00Z",
                        "acquisition_end_time": "2023-01-01T13:00:00Z",
                        "subject_details": details,
                    }
                    if strict:
                        with self.assertRaises(ValidationError):
                            self.enrich(acquisition, raise_if_invalid=True)
                    else:
                        with self.assertLogs(level="WARNING"):
                            self.enrich(acquisition)
                    self.assertIs(acquisition["subject_details"], details)
        self.assertEqual(mock_metadata_service_helper.call_count, 6)

    @patch.object(AcquisitionSubjectDetails, "model_validate")
    @patch("aind_metadata_mapper.enrichment.metadata_service_helper", return_value=[])
    def test_does_not_validate_incoming_details(self, mock_metadata_service_helper, mock_validate):
        """Incoming details are not validated when there are no weights to apply."""
        acquisition = {
            "acquisition_start_time": "2023-01-01T12:00:00Z",
            "acquisition_end_time": "2023-01-01T13:00:00Z",
            "subject_details": {},
        }
        self.assertIs(self.enrich(acquisition, raise_if_invalid=True), acquisition)
        self.assertEqual(acquisition["subject_details"], {})
        mock_metadata_service_helper.assert_called_once()
        mock_validate.assert_not_called()

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_invalid_weight_respects_raise_if_invalid(self, mock_metadata_service_helper):
        """Invalid fetched weights leave both acquisition weights unchanged."""
        mock_metadata_service_helper.return_value = [
            {"weight": 21, "weight_datetime": "2023-01-01T12:00:00Z"},
            {"weight": "invalid", "weight_datetime": "2023-01-01T13:00:00Z"},
        ]
        for strict in (False, True):
            with self.subTest(strict=strict):
                details = {"mouse_platform_name": "wheel"}
                acquisition = {
                    "acquisition_start_time": "2023-01-01T12:00:00Z",
                    "acquisition_end_time": "2023-01-01T13:00:00Z",
                    "subject_details": details,
                }
                if strict:
                    with self.assertRaises(ValidationError):
                        self.enrich(acquisition, raise_if_invalid=True)
                else:
                    with self.assertLogs(level="WARNING"):
                        self.enrich(acquisition)
                self.assertIs(acquisition["subject_details"], details)
                self.assertEqual(details, {"mouse_platform_name": "wheel"})

    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_mismatch_warns_if_invalid(self, mock_metadata_service_helper):
        """Weight conflicts are warnings even in strict validation mode."""
        details = {"mouse_platform_name": "wheel", "animal_weight_post": 25}
        acquisition = {
            "acquisition_start_time": "2023-01-01T12:00:00Z",
            "acquisition_end_time": "2023-01-01T13:00:00Z",
            "subject_details": details,
        }
        mock_metadata_service_helper.return_value = [
            {"weight": 21, "weight_datetime": "2023-01-01T12:00:00Z"},
            {"weight": 24, "weight_datetime": "2023-01-01T13:00:00Z"},
        ]
        with self.assertLogs(level="WARNING") as captured:
            self.enrich(acquisition, raise_if_invalid=True)
        self.assertIn("animal_weight_post", captured.output[0])
        validated_details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(validated_details.animal_weight_prior, 21)
        self.assertEqual(validated_details.animal_weight_post, 25)
        self.assertEqual(details, {"mouse_platform_name": "wheel", "animal_weight_post": 25})

    @patch("logging.warning")
    @patch("aind_metadata_mapper.enrichment.metadata_service_helper")
    def test_datetime_errors_are_not_validation_errors(self, mock_metadata_service_helper, mock_warning):
        """Datetime parsing errors propagate independently of the validation flag."""
        for strict in (False, True):
            with self.subTest(strict=strict):
                acquisition = {
                    "acquisition_start_time": "invalid",
                    "acquisition_end_time": "2023-01-01T13:00:00Z",
                    "subject_details": {"mouse_platform_name": "wheel"},
                }
                with self.assertRaises(ValueError):
                    self.enrich(acquisition, raise_if_invalid=strict)
        mock_warning.assert_not_called()
        mock_metadata_service_helper.assert_not_called()


class TestAddEthicsReviewId(unittest.TestCase):
    """Tests add_ethics_review_id"""

    def enrich(self, acquisition, replace_existing=False):
        """Run add_ethics_review_id against the test service for a mouse subject."""
        return add_ethics_review_id(
            acquisition=acquisition,
            subject=mouse_subject(),
            subject_id=SUBJECT_ID,
            metadata_service_url=SERVICE_URL,
            replace_existing=replace_existing,
        )

    @patch("aind_metadata_mapper.enrichment.get_iacuc_protocol", return_value="2414")
    def test_fills_when_missing(self, mock_get_iacuc_protocol):
        """Test that a missing, None, or empty ethics_review_id is filled from LabTracks."""
        for acquisition in ({}, {"ethics_review_id": None}, {"ethics_review_id": []}):
            with self.subTest(acquisition=acquisition):
                result = self.enrich(acquisition)
                self.assertIs(result, acquisition)
                self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_get_iacuc_protocol.assert_called_with(SUBJECT_ID, base_url=LABTRACKS_URL)

    @patch("aind_metadata_mapper.enrichment.get_iacuc_protocol", return_value=None)
    def test_warns_and_leaves_acquisition_unchanged_when_not_found(self, mock_get_iacuc_protocol):
        """Test that a failed or empty lookup is a warning and changes nothing."""
        acquisition = {"subject_id": SUBJECT_ID}
        with self.assertLogs(level="WARNING") as captured:
            result = self.enrich(acquisition)
        self.assertIs(result, acquisition)
        self.assertEqual(acquisition, {"subject_id": SUBJECT_ID})
        self.assertIn("ethics_review_id", captured.output[0])

    @patch("logging.warning")
    @patch("aind_metadata_mapper.enrichment.get_iacuc_protocol", return_value="2414")
    def test_keeps_matching_existing_value_without_warning(self, mock_get_iacuc_protocol, mock_warning):
        """Test that a matching existing value is kept silently."""
        acquisition = {"ethics_review_id": ["2414"]}
        self.enrich(acquisition)
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_warning.assert_not_called()

    @patch("aind_metadata_mapper.enrichment.get_iacuc_protocol", return_value="2414")
    def test_keeps_mismatched_existing_value_and_warns(self, mock_get_iacuc_protocol):
        """Test that a mismatched existing value is kept with a warning rather than raising."""
        acquisition = {"ethics_review_id": ["9999"]}
        with self.assertLogs(level="WARNING") as captured:
            self.enrich(acquisition)
        self.assertEqual(acquisition["ethics_review_id"], ["9999"])
        self.assertIn("9999", captured.output[0])
        self.assertIn("2414", captured.output[0])

    @patch("aind_metadata_mapper.enrichment.get_iacuc_protocol", return_value="2414")
    def test_replace_existing_overwrites(self, mock_get_iacuc_protocol):
        """Test that replace_existing=True overwrites a mismatched existing value."""
        acquisition = {"ethics_review_id": ["9999"]}
        self.enrich(acquisition, replace_existing=True)
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])


@patch("aind_metadata_mapper.enrichment.get_iacuc_protocol", return_value="2414")
@patch("aind_metadata_mapper.enrichment.metadata_service_helper", return_value=FIXTURE_WEIGHT_RECORDS)
class TestEnrichAcquisition(unittest.TestCase):
    """Tests enrich_acquisition"""

    def enrich(self, acquisition, subject, settings=None, **kwargs):
        """Run enrich_acquisition against the test service."""
        return enrich_acquisition(
            acquisition=acquisition,
            subject=subject,
            subject_id=FIXTURE_SUBJECT_ID,
            settings=settings or EnrichmentSettings(),
            metadata_service_url=SERVICE_URL,
            **kwargs,
        )

    def test_returns_none_for_missing_acquisition(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that a None acquisition is returned as is and no service is called."""
        self.assertIsNone(self.enrich(None, fixture_subject()))
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_disabled_returns_acquisition_unchanged(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that settings.enabled=False returns the acquisition untouched and calls no service."""
        acquisition = fixture_acquisition()
        expected = copy.deepcopy(acquisition)

        result = self.enrich(acquisition, fixture_subject(), EnrichmentSettings(enabled=False))

        self.assertIs(result, acquisition)
        self.assertEqual(acquisition, expected)
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_skips_non_mouse_subjects(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that missing, calibration, and non-mouse subjects skip enrichment entirely."""
        for subject in (
            None,
            {},
            {"subject_id": "calibration", "subject_details": {"object_type": "Calibration object"}},
            {"subject_id": SUBJECT_ID, "subject_details": {"object_type": "Human subject"}},
        ):
            with self.subTest(subject=subject):
                acquisition = fixture_acquisition()
                expected = copy.deepcopy(acquisition)
                self.assertIs(self.enrich(acquisition, subject), acquisition)
                self.assertEqual(acquisition, expected)
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_runs_all_enrichers_and_is_idempotent(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that both enrichers run, both fields land, and a second pass changes nothing and warns about nothing."""
        acquisition = fixture_acquisition()

        with patch("logging.warning") as mock_warning:
            first = self.enrich(acquisition, fixture_subject())

        self.assertIs(first, acquisition)
        self.assertEqual(acquisition["subject_details"]["animal_weight_prior"], "20.5")
        self.assertEqual(acquisition["subject_details"]["animal_weight_post"], "21.5")
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_warning.assert_not_called()
        mock_metadata_service_helper.assert_called_once_with(FIXTURE_WEIGHT_RECORDS_URL)
        mock_get_iacuc_protocol.assert_called_once_with(FIXTURE_SUBJECT_ID, base_url=LABTRACKS_URL)

        expected = copy.deepcopy(acquisition)
        with patch("logging.warning") as mock_warning:
            second = self.enrich(acquisition, fixture_subject())
        self.assertEqual(second, expected)
        mock_warning.assert_not_called()

    def test_replace_existing_is_passed_to_enrichers(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that settings.replace_existing makes every enricher overwrite existing values."""
        acquisition = fixture_acquisition()
        acquisition["subject_details"].update(animal_weight_prior="19.0", animal_weight_post="25.0")
        acquisition["ethics_review_id"] = ["9999"]

        self.enrich(acquisition, fixture_subject(), EnrichmentSettings(replace_existing=True))

        self.assertEqual(acquisition["subject_details"]["animal_weight_prior"], "20.5")
        self.assertEqual(acquisition["subject_details"]["animal_weight_post"], "21.5")
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])

    def test_invalid_input_warns_before_and_after_or_raises(
        self, mock_metadata_service_helper, mock_get_iacuc_protocol
    ):
        """An input that is not a valid Acquisition warns before and after enriching, or raises first when strict."""
        acquisition = mouse_acquisition()
        with self.assertLogs(level="WARNING") as captured:
            self.enrich(acquisition, mouse_subject())
        self.assertIn("before enrichment", captured.output[0])
        self.assertIn("after enrichment", captured.output[-1])
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_metadata_service_helper.assert_called_once()

        mock_metadata_service_helper.reset_mock()
        with self.assertRaises(ValidationError):
            self.enrich(mouse_acquisition(), mouse_subject(), raise_if_invalid=True)
        mock_metadata_service_helper.assert_not_called()

    def test_validate_false_skips_validation(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """validate=False enriches an input that is not a valid Acquisition without warning or raising."""
        acquisition = mouse_acquisition()
        with patch("logging.warning") as mock_warning:
            self.enrich(acquisition, mouse_subject(), raise_if_invalid=True, validate=False)
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_warning.assert_not_called()

    def test_enricher_that_breaks_validity_is_caught(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """If an enricher leaves the acquisition invalid, the validation after enriching reports it."""

        def break_subject_id(acquisition, **kwargs):
            """Enricher stand-in that violates the schema."""
            acquisition["subject_id"] = None
            return acquisition

        with patch("aind_metadata_mapper.enrichment.ENRICHERS", [break_subject_id]):
            with self.assertLogs(level="WARNING") as captured:
                self.enrich(fixture_acquisition(), fixture_subject())
            self.assertEqual(len(captured.output), 1)
            self.assertIn("after enrichment", captured.output[0])
            with self.assertRaises(ValidationError):
                self.enrich(fixture_acquisition(), fixture_subject(), raise_if_invalid=True)


class TestValidateAndWriteAcquisition(unittest.TestCase):
    """Tests validate_acquisition and write_acquisition"""

    def test_validate_acquisition_returns_true_when_valid(self):
        """A valid Acquisition passes silently."""
        with patch("logging.warning") as mock_warning:
            self.assertTrue(validate_acquisition(fixture_acquisition()))
        mock_warning.assert_not_called()

    def test_validate_acquisition_invalid_warns_or_raises(self):
        """An invalid Acquisition warns with the given context, or raises when strict."""
        with self.assertLogs(level="WARNING") as captured:
            self.assertFalse(validate_acquisition({}, context="in a test"))
        self.assertIn("in a test", captured.output[0])
        with self.assertRaises(ValidationError):
            validate_acquisition({}, raise_if_invalid=True)

    def test_write_acquisition_writes_standard_file(self):
        """The acquisition is written as acquisition.json in a directory that is created if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_directory = Path(tmpdir) / "nested"
            written = write_acquisition(fixture_acquisition(), output_directory)
            self.assertEqual(written, output_directory / Acquisition.default_filename())
            with written.open() as resource:
                contents = json.load(resource)
        Acquisition.model_validate(contents)
        self.assertEqual(contents["subject_id"], FIXTURE_SUBJECT_ID)

    def test_write_acquisition_rejects_invalid(self):
        """An invalid acquisition raises and nothing is written."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValidationError):
                write_acquisition({}, Path(tmpdir) / "nested")
            self.assertEqual(os.listdir(tmpdir), [])


if __name__ == "__main__":
    unittest.main()
