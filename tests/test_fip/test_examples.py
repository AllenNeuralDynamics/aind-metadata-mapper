"""Tests for FIP example workflows."""

import unittest
from aind_metadata_mapper.fip.examples.fip_etl import run_workflow


class TestFipExamples(unittest.TestCase):
    """Test FIP example workflows."""

    def test_fip_workflow(self):
        """Test that example workflow runs without error."""
        run_workflow(dry_run=True)


if __name__ == "__main__":
    unittest.main()
