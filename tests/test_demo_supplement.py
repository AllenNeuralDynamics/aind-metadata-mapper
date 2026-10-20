"""Tests for the supplement demo CLI."""

import json
import runpy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_data_schema.core.acquisition import Acquisition

PROJECT_DIR = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_DIR / "scripts" / "demo_supplement.py"
V2_METADATA_DIR = PROJECT_DIR / "tests" / "resources" / "v2_metadata"
WEIGHT_RECORDS = [
    {"weight": 20.5, "weight_datetime": "2025-09-17T17:00:00Z"},
    {"weight": 21.5, "weight_datetime": "2025-09-17T18:45:00Z"},
]
SUPPLEMENTED = {
    "subject_details.animal_weight_prior": "20.5",
    "subject_details.animal_weight_post": "21.5",
    "ethics_review_id": ["2414"],
}


def run_script(*arguments, records):
    """Run the demo with DocDB and metadata service calls mocked. Returns the requests.get mock and printed lines."""
    response = MagicMock()
    response.json.return_value = records
    with (
        patch("sys.argv", [str(SCRIPT), *arguments]),
        patch("requests.get", return_value=response) as mock_get,
        patch("aind_metadata_mapper.supplement.metadata_service_helper", return_value=WEIGHT_RECORDS),
        patch("aind_metadata_mapper.supplement.get_iacuc_protocol", return_value="2414"),
        patch("builtins.print") as mock_print,
    ):
        runpy.run_path(str(SCRIPT), run_name="__main__")
    return mock_get, [call.args[0] for call in mock_print.call_args_list]


class TestDemoSupplement(unittest.TestCase):
    """Check the demo fetches a session by name, reports what supplement filled, and can write the result."""

    def setUp(self):
        """Build a DocDB-shaped record from the acquisition fixture, with ethics_review_id removed."""
        with (V2_METADATA_DIR / "acquisition.json").open() as resource:
            acquisition = json.load(resource)
        del acquisition["ethics_review_id"]
        self.name = "behavior_804670_2025-09-17_10-26-00"
        self.record = {
            "name": self.name,
            "acquisition": acquisition,
            "data_description": {"modalities": [{"name": "Behavior", "abbreviation": "behavior"}]},
        }

    def test_prints_before_and_after(self):
        """The named record's acquisition is supplemented and the before and after values are printed."""
        mock_get, printed = run_script(self.name, records=[self.record])

        self.assertEqual(
            mock_get.call_args.args[0], "https://api.allenneuraldynamics.org/v2/metadata_index/data_assets/find"
        )
        self.assertEqual(json.loads(mock_get.call_args.kwargs["params"]["filter"]), {"name": self.name})
        self.assertEqual(
            json.loads(mock_get.call_args.kwargs["params"]["projection"]),
            {"name": 1, "acquisition": 1, "data_description.modalities": 1},
        )
        self.assertEqual(len(printed), 1)
        output = json.loads(printed[0])
        self.assertEqual(output["name"], self.name)
        self.assertEqual(output["modalities"], ["behavior"])
        self.assertEqual(output["before"], {field: None for field in SUPPLEMENTED})
        self.assertEqual(output["after"], SUPPLEMENTED)

    def test_post_mortem_record_is_left_unchanged(self):
        """A record whose only modality is SPIM is printed with identical, empty before and after values."""
        self.record["data_description"] = {
            "modalities": [{"name": "Selective plane illumination microscopy", "abbreviation": "SPIM"}]
        }

        _, printed = run_script(self.name, records=[self.record])

        output = json.loads(printed[0])
        self.assertEqual(output["modalities"], ["SPIM"])
        self.assertEqual(output["before"], {field: None for field in SUPPLEMENTED})
        self.assertEqual(output["after"], output["before"])

    def test_output_dir_writes_supplemented_acquisition(self):
        """With --output-dir the supplemented acquisition is written as a standard acquisition.json."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "out"
            _, printed = run_script(self.name, "--output-dir", str(output_dir), records=[self.record])
            written = output_dir / "acquisition.json"
            self.assertEqual(printed[1], f"Wrote {written}")
            with written.open() as resource:
                contents = json.load(resource)
        Acquisition.model_validate(contents)
        self.assertEqual(contents["subject_details"]["animal_weight_prior"], "20.5")
        self.assertEqual(contents["subject_details"]["animal_weight_post"], "21.5")
        self.assertEqual(contents["ethics_review_id"], ["2414"])

    def test_unknown_session_exits(self):
        """A name with no DocDB record exits with a message instead of a traceback."""
        with self.assertRaises(SystemExit) as context:
            run_script("not_a_session", records=[])
        self.assertIn("not_a_session", str(context.exception))
