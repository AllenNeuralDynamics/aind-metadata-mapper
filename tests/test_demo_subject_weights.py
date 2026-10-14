"""Tests for the subject weight demo CLI."""

import json
import runpy
import unittest
from pathlib import Path
from unittest.mock import patch


class TestDemoSubjectWeights(unittest.TestCase):
    """Check acquisition details are reused without a platform CLI argument."""

    def test_demo_reuses_acquisition_details(self):
        """Both the default session and supplied JSON provide their own platform."""
        project_dir = Path(__file__).resolve().parents[1]
        script = project_dir / "scripts" / "demo_subject_weights.py"
        acquisition_file = project_dir / "tests" / "resources" / "v2_metadata" / "acquisition.json"
        with acquisition_file.open() as resource:
            stored_acquisition = json.load(resource)
        for arguments, expected_details, expected_subject in (
            ([], {"mouse_platform_name": "wheel"}, "864846"),
            (
                ["--acquisition-file", str(acquisition_file)],
                stored_acquisition["subject_details"],
                stored_acquisition["subject_id"],
            ),
        ):
            with self.subTest(arguments=arguments):
                with (
                    patch("sys.argv", [str(script), *arguments]),
                    patch("aind_metadata_mapper.gather_metadata.GatherMetadataJob") as mock_job_class,
                    patch("builtins.print") as mock_print,
                ):
                    job = mock_job_class.return_value
                    job.get_subject.return_value = {"subject_id": expected_subject}
                    runpy.run_path(str(script), run_name="__main__")
                    job.get_subject.assert_called_once_with(subject_id=expected_subject)
                    acquisition = job.add_subject_weights.call_args.args[1]
                    self.assertEqual(acquisition["subject_details"], expected_details)
                    self.assertEqual(job.add_subject_weights.call_args.args[2], expected_subject)
                    output = json.loads(mock_print.call_args_list[0].args[0])
                    self.assertEqual(output["acquisition_start_time"], acquisition["acquisition_start_time"])
                    self.assertEqual(output["acquisition_end_time"], acquisition["acquisition_end_time"])