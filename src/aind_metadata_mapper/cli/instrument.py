"""CLI for instrument database operations.

Provides ``upload`` and ``get`` subcommands for working with the AIND
instrument metadata service.

Usage::

    aind-instrument upload instrument.json
    aind-instrument upload instrument.json --replace
    aind-instrument upload instrument.json --no-update-modification-date  # keep date from file
    aind-instrument get 422_MESO2_20241017
    aind-instrument get 422_MESO2_20241017 --modification-date 2024-10-28
    aind-instrument get 422_MESO2_20241017 --output-directory ./output
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field
from pydantic_settings import CliApp, CliPositionalArg, CliSubCommand

from aind_metadata_mapper.utils import INSTRUMENT_BASE_URL, get_instrument, save_instrument

logger = logging.getLogger(__name__)

BASE_URL_FIELD = Field(
    default=INSTRUMENT_BASE_URL,
    description="Base URL for the instrument metadata service.",
)


class Upload(BaseModel):
    """Upload an instrument JSON file to the metadata service database."""

    file: CliPositionalArg[Path] = Field(
        description="Path to an instrument JSON file to upload.",
    )
    replace: bool = Field(
        default=False,
        description=("If set, overwrite an existing record with the same " "instrument_id and modification_date."),
    )
    update_modification_date: bool = Field(
        default=True,
        description=(
            "Update modification_date to today's date."
            " Use --no-update-modification-date to keep the date from the file."
        ),
    )
    base_url: str = BASE_URL_FIELD

    def cli_cmd(self) -> None:
        """Upload an instrument JSON to the database."""
        try:
            save_instrument(
                self.file,
                replace=self.replace,
                update_modification_date=self.update_modification_date,
                base_url=self.base_url,
            )
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            sys.exit(1)


class Get(BaseModel):
    """Retrieve an instrument record from the metadata service database."""

    instrument_id: CliPositionalArg[str] = Field(
        description="Instrument identifier to look up.",
    )
    modification_date: Optional[str] = Field(
        default=None,
        description=("Specific modification date (YYYY-MM-DD) to retrieve. " "If omitted, returns the latest record."),
    )
    output_directory: Optional[Path] = Field(
        default=None,
        description=("Directory to save the instrument JSON file. " "If omitted, prints the JSON to stdout."),
    )
    base_url: str = BASE_URL_FIELD

    def cli_cmd(self) -> None:
        """Get an instrument record and display or save it."""
        result = get_instrument(
            self.instrument_id,
            modification_date=self.modification_date,
            output_directory=self.output_directory,
            base_url=self.base_url,
        )
        if result is None:
            logger.error(f"Instrument '{self.instrument_id}' not found.")
            sys.exit(1)
        if self.output_directory is None:
            print(result.model_dump_json(indent=3))


class InstrumentCLI(BaseModel):
    """CLI for instrument database operations."""

    upload: CliSubCommand[Upload]
    get: CliSubCommand[Get]

    def cli_cmd(self) -> None:
        """Dispatch to the active subcommand."""
        CliApp.run_subcommand(self)


def main() -> None:
    """Entry point for the aind-instrument CLI."""
    CliApp.run(InstrumentCLI)


if __name__ == "__main__":
    main()
