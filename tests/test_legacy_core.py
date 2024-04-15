"""Tests legacy BaseEtl class methods. We can remove this once the other jobs
 are ported over."""

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest import TestCase
from unittest import main as unittest_main
from unittest.mock import MagicMock, patch

from aind_data_schema.core.subject import BreedingInfo, Housing, Sex, Subject
from aind_data_schema.models.organizations import Organization
from aind_data_schema.models.species import Species

from aind_metadata_mapper.core import BaseEtl


class TestBaseEtl(TestCase):
    """Tests methods in the legacy BaseEtl class."""

    class LegacyEtl(BaseEtl):
        """Mock a child class"""

        def _extract(self) -> None:
            """Mocked extract method"""
            return None

        def _transform(self, extracted_source: Any = None) -> Subject:
            """Mocked transform method to return an invalid or valid subject
            model"""
            if self.input_source != "valid_source":
                return Subject.model_construct()
            else:
                t = datetime(2022, 11, 22, 8, 43, 00)

                s = Subject(
                    species=Species.MUS_MUSCULUS,
                    subject_id="12345",
                    sex=Sex.MALE,
                    date_of_birth=t.date(),
                    source=Organization.AI,
                    breeding_info=BreedingInfo(
                        breeding_group="Emx1-IRES-Cre(ND)",
                        maternal_id="546543",
                        maternal_genotype=(
                            "Emx1-IRES-Cre/wt; Camk2a-tTa/Camk2a-tTA"
                        ),
                        paternal_id="232323",
                        paternal_genotype="Ai93(TITL-GCaMP6f)/wt",
                    ),
                    genotype=(
                        "Emx1-IRES-Cre/wt;Camk2a-tTA/wt;Ai93(TITL-GCaMP6f)/wt"
                    ),
                    housing=Housing(
                        home_cage_enrichment=["Running wheel"], cage_id="123"
                    ),
                    background_strain="C57BL/6J",
                )
                return s

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch("logging.warning")
    def test_legacy_invalid_model(
        self, mock_log_warning: MagicMock, mock_write: MagicMock
    ):
        """Tests run_job when an invalid model is created."""
        etl_job = self.LegacyEtl(
            input_source="source", output_directory=Path("out")
        )
        etl_job.run_job()
        mock_log_warning.assert_called_once()
        mock_write.assert_called_once_with(output_directory=Path("out"))

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch("logging.debug")
    def test_legacy_valid_model(
        self, mock_log_debug: MagicMock, mock_write: MagicMock
    ):
        """Tests run_job when a valid model is created."""

        etl_job = self.LegacyEtl(
            input_source="valid_source", output_directory=Path("out")
        )
        etl_job.run_job()
        mock_log_debug.assert_called_once_with(
            "No validation errors detected."
        )
        mock_write.assert_called_once_with(output_directory=Path("out"))


if __name__ == "__main__":
    unittest_main()
