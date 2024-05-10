"""Tests for the neuropixels open ephys rig ETL with inferred probe mapping."""

import os
import unittest
from datetime import date
from pathlib import Path

from aind_metadata_mapper.dynamic_routing.neuropixels_rig import (
    NeuropixelsRigEtl,
)

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "neuropixels"
)


class TestNeuropixelsRig(unittest.TestCase):
    """Tests dxdiag utilities in for the neuropixels project."""

    def test_update_modification_date(self):
        """Test ETL workflow with inferred probe mapping."""
        etl = NeuropixelsRigEtl(
            RESOURCES_DIR / "base_rig.json",
            Path("./"),
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        NeuropixelsRigEtl.update_modification_date(transformed)
        assert transformed.modification_date == date.today()


if __name__ == "__main__":
    unittest.main()
