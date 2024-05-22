"""Tests for Sync rig ETL."""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_metadata_mapper.dynamic_routing.sync_rig import (  # type: ignore
    SyncRigEtl,
)
from tests.test_dynamic_routing import test_utils as test_utils

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "dynamic_routing"
)


class TestSyncRigEtl(unittest.TestCase):
    """Tests dxdiag utilities in for the dynamic_routing project."""

    def test_transform(self):
        """Test etl transform."""
        etl = SyncRigEtl(
            self.input_source,
            self.output_dir,
            RESOURCES_DIR / "sync.yml",
            modification_date=self.expected.modification_date,
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        self.assertEqual(transformed, self.expected)

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_etl(
        self,
        mock_write_standard_file: MagicMock,
    ):
        """Test ETL workflow."""
        etl = SyncRigEtl(
            self.input_source,
            self.output_dir,
            RESOURCES_DIR / "sync.yml",
        )
        etl.run_job()
        mock_write_standard_file.assert_called_once_with(
            output_directory=self.output_dir
        )

    def setUp(self):
        """Sets up test resources."""
        (
            self.input_source,
            self.output_dir,
            self.expected,
        ) = test_utils.setup_neuropixels_etl_resources(
            RESOURCES_DIR / "sync_rig.json"
        )


if __name__ == "__main__":
    unittest.main()
