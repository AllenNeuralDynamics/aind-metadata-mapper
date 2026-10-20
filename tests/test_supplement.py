"""Tests for the acquisition supplement module."""

import copy
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from aind_data_schema.core.acquisition import AcquisitionSubjectDetails
from aind_data_schema_models.modalities import Modality
from pydantic import ValidationError

from aind_metadata_mapper.models import SupplementSettings
from aind_metadata_mapper.supplement import (
    IN_VIVO_MODALITIES,
    add_ethics_review_id,
    add_subject_weights,
    supplement_acquisition,
)

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
IN_VIVO_DATA_DESCRIPTION = {"modalities": [{"name": "Behavior", "abbreviation": "behavior"}]}
POST_MORTEM_DATA_DESCRIPTION = {
    "modalities": [{"name": "Selective plane illumination microscopy", "abbreviation": "SPIM"}]
}


def mouse_acquisition(**subject_details) -> dict:
    """Return a minimal acquisition for a wheel session starting at noon UTC. Not a valid Acquisition."""
    return {
        "acquisition_start_time": "2023-01-01T12:00:00+00:00",
        "acquisition_end_time": "2023-01-01T13:00:00+00:00",
        "subject_details": {"mouse_platform_name": "wheel", **subject_details},
    }


def fixture_acquisition() -> dict:
    """Return the valid v2 acquisition fixture with ethics_review_id removed so supplement has to fill it."""
    with (V2_METADATA_DIR / "acquisition.json").open() as resource:
        acquisition = json.load(resource)
    del acquisition["ethics_review_id"]
    return acquisition


class TestAddSubjectWeights(unittest.TestCase):
    """Tests add_subject_weights"""

    def supplement(self, acquisition, raise_if_invalid=False, replace_existing=False):
        """Run add_subject_weights against the test service for a mouse subject."""
        return add_subject_weights(
            acquisition=acquisition,
            subject_id=SUBJECT_ID,
            metadata_service_url=SERVICE_URL,
            raise_if_invalid=raise_if_invalid,
            replace_existing=replace_existing,
        )

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
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

        result = self.supplement(acquisition)

        self.assertIs(result, acquisition)
        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 21)
        self.assertEqual(details.animal_weight_post, 22)
        self.assertEqual(acquisition["subject_details"], details.model_dump(mode="json"))
        mock_metadata_service_helper.assert_called_once_with(WEIGHT_RECORDS_URL)

    @patch("aind_metadata_mapper.supplement.metadata_service_helper", return_value=[])
    def test_without_matching_captures_warns_and_does_not_add_weights(self, mock_metadata_service_helper):
        """Test that no available captures leave pre and post weights unset, with a warning."""
        acquisition = mouse_acquisition()

        with self.assertLogs(level="WARNING") as captured:
            result = self.supplement(acquisition)

        self.assertIs(result, acquisition)
        self.assertEqual(acquisition["subject_details"], {"mouse_platform_name": "wheel"})
        self.assertIn("No Waterlog weight records", captured.output[0])
        mock_metadata_service_helper.assert_called_once()

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
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

        self.supplement(acquisition)

        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 22)
        self.assertEqual(details.animal_weight_post, 23)
        mock_metadata_service_helper.assert_called_once()

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
    def test_without_acquisition_start_does_not_query(self, mock_metadata_service_helper):
        """Test that an acquisition without a start time is ignored."""
        acquisition = {"acquisition_end_time": "2023-01-01T13:00:00+00:00"}

        result = self.supplement(acquisition)

        self.assertIs(result, acquisition)
        self.assertEqual(acquisition, {"acquisition_end_time": "2023-01-01T13:00:00+00:00"})
        mock_metadata_service_helper.assert_not_called()

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
    def test_ignores_invalid_service_records(self, mock_metadata_service_helper):
        """Test that malformed weight records are skipped."""
        acquisition = mouse_acquisition()
        mock_metadata_service_helper.return_value = [
            None,
            {},
            {"weight": 22.0, "weight_datetime": "not-a-datetime"},
        ]

        self.supplement(acquisition)

        self.assertEqual(acquisition["subject_details"], {"mouse_platform_name": "wheel"})
        mock_metadata_service_helper.assert_called_once()

    @patch("aind_metadata_mapper.supplement.metadata_service_helper", return_value={"records": []})
    def test_ignores_non_list_service_response(self, mock_metadata_service_helper):
        """Test that a non-list service response leaves subject weights unchanged."""
        acquisition = mouse_acquisition()

        self.supplement(acquisition)

        self.assertEqual(acquisition["subject_details"], {"mouse_platform_name": "wheel"})
        mock_metadata_service_helper.assert_called_once()

    @patch("logging.warning")
    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
    def test_preserves_existing_values_and_warns_on_mismatch(self, mock_metadata_service_helper, mock_warning):
        """Test that existing weights are preserved and mismatches are warned about."""
        acquisition = mouse_acquisition(animal_weight_prior=19.0, animal_weight_post=25.0)
        mock_metadata_service_helper.return_value = [
            {"weight": 21.0, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 24.0, "weight_datetime": "2023-01-01T13:00:01Z"},
        ]

        self.supplement(acquisition)

        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 19)
        self.assertEqual(details.animal_weight_post, 25)
        self.assertEqual(mock_warning.call_count, 2)
        self.assertIn("animal_weight_prior", mock_warning.call_args_list[0].args[0])
        self.assertIn("animal_weight_post", mock_warning.call_args_list[1].args[0])

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
    def test_replace_existing_overwrites_existing_values(self, mock_metadata_service_helper):
        """Test that replace_existing=True overwrites existing weights with the fetched values."""
        acquisition = mouse_acquisition(animal_weight_prior=19.0, animal_weight_post=25.0)
        mock_metadata_service_helper.return_value = [
            {"weight": 21.0, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 24.0, "weight_datetime": "2023-01-01T13:00:01Z"},
        ]

        self.supplement(acquisition, replace_existing=True)

        details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(details.animal_weight_prior, 21)
        self.assertEqual(details.animal_weight_post, 24)

    @patch("logging.warning")
    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
    def test_does_not_warn_when_existing_values_match(self, mock_metadata_service_helper, mock_warning):
        """Test that matching existing weights are retained without warnings."""
        acquisition = mouse_acquisition(animal_weight_prior="21.1", animal_weight_post="24.1")
        mock_metadata_service_helper.return_value = [
            {"weight": 21.1, "weight_datetime": "2023-01-01T11:59:59Z"},
            {"weight": 24.1, "weight_datetime": "2023-01-01T13:00:01Z"},
        ]

        self.supplement(acquisition)

        self.assertEqual(acquisition["subject_details"]["animal_weight_prior"], "21.1")
        self.assertEqual(acquisition["subject_details"]["animal_weight_post"], "24.1")
        mock_warning.assert_not_called()

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
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
                            self.supplement(acquisition, raise_if_invalid=True)
                    else:
                        with self.assertLogs(level="WARNING"):
                            self.supplement(acquisition)
                    self.assertIs(acquisition["subject_details"], details)
        self.assertEqual(mock_metadata_service_helper.call_count, 6)

    @patch.object(AcquisitionSubjectDetails, "model_validate")
    @patch("aind_metadata_mapper.supplement.metadata_service_helper", return_value=[])
    def test_does_not_validate_incoming_details(self, mock_metadata_service_helper, mock_validate):
        """Incoming details are not validated when there are no weights to apply."""
        acquisition = {
            "acquisition_start_time": "2023-01-01T12:00:00Z",
            "acquisition_end_time": "2023-01-01T13:00:00Z",
            "subject_details": {},
        }
        self.assertIs(self.supplement(acquisition, raise_if_invalid=True), acquisition)
        self.assertEqual(acquisition["subject_details"], {})
        mock_metadata_service_helper.assert_called_once()
        mock_validate.assert_not_called()

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
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
                        self.supplement(acquisition, raise_if_invalid=True)
                else:
                    with self.assertLogs(level="WARNING"):
                        self.supplement(acquisition)
                self.assertIs(acquisition["subject_details"], details)
                self.assertEqual(details, {"mouse_platform_name": "wheel"})

    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
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
            self.supplement(acquisition, raise_if_invalid=True)
        self.assertIn("animal_weight_post", captured.output[0])
        validated_details = AcquisitionSubjectDetails.model_validate(acquisition["subject_details"])
        self.assertEqual(validated_details.animal_weight_prior, 21)
        self.assertEqual(validated_details.animal_weight_post, 25)
        self.assertEqual(details, {"mouse_platform_name": "wheel", "animal_weight_post": 25})

    @patch("logging.warning")
    @patch("aind_metadata_mapper.supplement.metadata_service_helper")
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
                    self.supplement(acquisition, raise_if_invalid=strict)
        mock_warning.assert_not_called()
        mock_metadata_service_helper.assert_not_called()


class TestAddEthicsReviewId(unittest.TestCase):
    """Tests add_ethics_review_id"""

    def supplement(self, acquisition, replace_existing=False):
        """Run add_ethics_review_id against the test service for a mouse subject."""
        return add_ethics_review_id(
            acquisition=acquisition,
            subject_id=SUBJECT_ID,
            metadata_service_url=SERVICE_URL,
            replace_existing=replace_existing,
        )

    @patch("aind_metadata_mapper.supplement.get_iacuc_protocol", return_value="2414")
    def test_fills_when_missing(self, mock_get_iacuc_protocol):
        """Test that a missing, None, or empty ethics_review_id is filled from LabTracks."""
        for acquisition in ({}, {"ethics_review_id": None}, {"ethics_review_id": []}):
            with self.subTest(acquisition=acquisition):
                result = self.supplement(acquisition)
                self.assertIs(result, acquisition)
                self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_get_iacuc_protocol.assert_called_with(SUBJECT_ID, base_url=LABTRACKS_URL)

    @patch("aind_metadata_mapper.supplement.get_iacuc_protocol", return_value=None)
    def test_warns_and_leaves_acquisition_unchanged_when_not_found(self, mock_get_iacuc_protocol):
        """Test that a failed or empty lookup is a warning and changes nothing."""
        acquisition = {"subject_id": SUBJECT_ID}
        with self.assertLogs(level="WARNING") as captured:
            result = self.supplement(acquisition)
        self.assertIs(result, acquisition)
        self.assertEqual(acquisition, {"subject_id": SUBJECT_ID})
        self.assertIn("ethics_review_id", captured.output[0])

    @patch("logging.warning")
    @patch("aind_metadata_mapper.supplement.get_iacuc_protocol", return_value="2414")
    def test_keeps_matching_existing_value_without_warning(self, mock_get_iacuc_protocol, mock_warning):
        """Test that a matching existing value is kept silently."""
        acquisition = {"ethics_review_id": ["2414"]}
        self.supplement(acquisition)
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_warning.assert_not_called()

    @patch("aind_metadata_mapper.supplement.get_iacuc_protocol", return_value="2414")
    def test_keeps_mismatched_existing_value_and_warns(self, mock_get_iacuc_protocol):
        """Test that a mismatched existing value is kept with a warning rather than raising."""
        acquisition = {"ethics_review_id": ["9999"]}
        with self.assertLogs(level="WARNING") as captured:
            self.supplement(acquisition)
        self.assertEqual(acquisition["ethics_review_id"], ["9999"])
        self.assertIn("9999", captured.output[0])
        self.assertIn("2414", captured.output[0])

    @patch("aind_metadata_mapper.supplement.get_iacuc_protocol", return_value="2414")
    def test_replace_existing_overwrites(self, mock_get_iacuc_protocol):
        """Test that replace_existing=True overwrites a mismatched existing value."""
        acquisition = {"ethics_review_id": ["9999"]}
        self.supplement(acquisition, replace_existing=True)
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])


@patch("aind_metadata_mapper.supplement.get_iacuc_protocol", return_value="2414")
@patch("aind_metadata_mapper.supplement.metadata_service_helper", return_value=FIXTURE_WEIGHT_RECORDS)
class TestSupplementAcquisition(unittest.TestCase):
    """Tests supplement_acquisition"""

    def supplement(
        self,
        acquisition,
        settings=None,
        subject_id=FIXTURE_SUBJECT_ID,
        data_description=IN_VIVO_DATA_DESCRIPTION,
        **kwargs,
    ):
        """Run supplement_acquisition against the test service, for an in vivo acquisition unless told otherwise."""
        return supplement_acquisition(
            acquisition=acquisition,
            data_description=data_description,
            subject_id=subject_id,
            settings=settings or SupplementSettings(),
            metadata_service_url=SERVICE_URL,
            **kwargs,
        )

    def test_returns_none_for_missing_acquisition(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that a None acquisition is returned as is and no service is called."""
        self.assertIsNone(self.supplement(None))
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_disabled_returns_acquisition_unchanged(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that settings.enabled=False returns the acquisition untouched and calls no service."""
        acquisition = fixture_acquisition()
        expected = copy.deepcopy(acquisition)

        result = self.supplement(acquisition, SupplementSettings(enabled=False))

        self.assertIs(result, acquisition)
        self.assertEqual(acquisition, expected)
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_skips_calibration_subject(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that a calibration run skips supplement entirely: no service calls, no changes."""
        acquisition = {"subject_id": "calibration", "acquisition_start_time": "2023-01-01T12:00:00+00:00"}
        expected = copy.deepcopy(acquisition)

        with patch("logging.warning") as mock_warning:
            self.assertIs(self.supplement(acquisition, subject_id="calibration"), acquisition)

        self.assertEqual(acquisition, expected)
        mock_warning.assert_not_called()
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_in_vivo_modalities_are_the_agreed_set(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that the in vivo list is exactly the agreed modalities, so a change to it is deliberate."""
        self.assertEqual(
            IN_VIVO_MODALITIES,
            {"behavior", "behavior-videos", "ecephys", "EMG", "fib", "ISI", "MRI", "one-photon", "pophys", "slap2"},
        )

    def test_skips_without_in_vivo_modality(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that a post-mortem acquisition (SPIM only) is left untouched, with no service calls and an INFO log."""
        acquisition = fixture_acquisition()
        expected = copy.deepcopy(acquisition)

        with self.assertLogs(level="INFO") as captured:
            result = self.supplement(acquisition, data_description=POST_MORTEM_DATA_DESCRIPTION)

        self.assertIs(result, acquisition)
        self.assertEqual(acquisition, expected)
        self.assertIn("no in vivo modality", captured.output[0])
        self.assertIn("SPIM", captured.output[0])
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_skips_without_data_description(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that a missing or empty data description means no lookups."""
        for data_description in (None, {}, {"modalities": []}):
            with self.subTest(data_description=data_description):
                acquisition = fixture_acquisition()
                expected = copy.deepcopy(acquisition)
                self.assertIs(self.supplement(acquisition, data_description=data_description), acquisition)
                self.assertEqual(acquisition, expected)
        mock_metadata_service_helper.assert_not_called()
        mock_get_iacuc_protocol.assert_not_called()

    def test_runs_when_any_modality_is_in_vivo(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that one in vivo modality next to a post-mortem one is enough, however the modalities are given."""
        for modalities in (
            [{"name": "Selective plane illumination microscopy", "abbreviation": "SPIM"}, {"abbreviation": "ecephys"}],
            ["SPIM", "ecephys"],
            [Modality.SPIM, Modality.ECEPHYS],
        ):
            with self.subTest(modalities=modalities):
                acquisition = fixture_acquisition()
                self.supplement(acquisition, data_description={"modalities": modalities})
                self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        self.assertEqual(mock_get_iacuc_protocol.call_count, 3)

    def test_runs_all_steps_and_is_idempotent(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that both steps run, both fields land, and a second pass changes nothing and warns about nothing."""
        acquisition = fixture_acquisition()

        with patch("logging.warning") as mock_warning:
            first = self.supplement(acquisition)

        self.assertIs(first, acquisition)
        self.assertEqual(acquisition["subject_details"]["animal_weight_prior"], "20.5")
        self.assertEqual(acquisition["subject_details"]["animal_weight_post"], "21.5")
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_warning.assert_not_called()
        mock_metadata_service_helper.assert_called_once_with(FIXTURE_WEIGHT_RECORDS_URL)
        mock_get_iacuc_protocol.assert_called_once_with(FIXTURE_SUBJECT_ID, base_url=LABTRACKS_URL)

        expected = copy.deepcopy(acquisition)
        with patch("logging.warning") as mock_warning:
            second = self.supplement(acquisition)
        self.assertEqual(second, expected)
        mock_warning.assert_not_called()

    def test_replace_existing_is_passed_to_steps(self, mock_metadata_service_helper, mock_get_iacuc_protocol):
        """Test that settings.replace_existing makes every step overwrite existing values."""
        acquisition = fixture_acquisition()
        acquisition["subject_details"].update(animal_weight_prior="19.0", animal_weight_post="25.0")
        acquisition["ethics_review_id"] = ["9999"]

        self.supplement(acquisition, SupplementSettings(replace_existing=True))

        self.assertEqual(acquisition["subject_details"]["animal_weight_prior"], "20.5")
        self.assertEqual(acquisition["subject_details"]["animal_weight_post"], "21.5")
        self.assertEqual(acquisition["ethics_review_id"], ["2414"])


if __name__ == "__main__":
    unittest.main()
