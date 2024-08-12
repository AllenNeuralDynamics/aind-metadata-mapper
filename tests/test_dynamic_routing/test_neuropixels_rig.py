"""Tests for the dynamic_routing neuropixel rig ETL inferred probe mapping."""

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
    / "dynamic_routing"
)


class TestNeuropixelsRig(unittest.TestCase):
    """Tests dxdiag utilities in for the dynamic_routing project."""

    def test_update_modification_date(self):
        """Test ETL workflow with inferred probe mapping."""
        etl = NeuropixelsRigEtl(
            RESOURCES_DIR / "base_rig.json",
            Path("abc"),
        )
        extracted = etl._extract()
        transformed = etl._transform(extracted)
        NeuropixelsRigEtl.update_modification_date(transformed)
        assert transformed.modification_date == date.today()


if __name__ == "__main__":
    unittest.main()
