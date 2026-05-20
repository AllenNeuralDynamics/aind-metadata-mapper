"""Tests for instrument CLI commands."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import aind_data_schema.core.instrument as instrument

from aind_metadata_mapper.cli.instrument import Get, Upload
from aind_metadata_mapper.utils import INSTRUMENT_BASE_URL

INSTRUMENT_JSON = Path(__file__).parent.parent / "resources" / "v2_metadata" / "instrument.json"


class TestUploadCmd(unittest.TestCase):
    """Tests for the Upload subcommand."""

    @patch("aind_metadata_mapper.cli.instrument.save_instrument")
    def test_upload_default_flags(self, mock_save):
        """Upload passes file path and default flags to save_instrument."""
        cmd = Upload(file=INSTRUMENT_JSON)
        cmd.cli_cmd()
        mock_save.assert_called_once_with(
            INSTRUMENT_JSON,
            replace=False,
            update_modification_date=True,
            base_url=INSTRUMENT_BASE_URL,
        )

    @patch("aind_metadata_mapper.cli.instrument.save_instrument")
    def test_upload_with_replace(self, mock_save):
        """Upload passes replace=True when flag is set."""
        cmd = Upload(file=INSTRUMENT_JSON, replace=True)
        cmd.cli_cmd()
        mock_save.assert_called_once_with(
            INSTRUMENT_JSON,
            replace=True,
            update_modification_date=True,
            base_url=INSTRUMENT_BASE_URL,
        )

    @patch("aind_metadata_mapper.cli.instrument.save_instrument")
    def test_upload_no_update_modification_date(self, mock_save):
        """Upload passes update_modification_date=False when flag is set."""
        cmd = Upload(file=INSTRUMENT_JSON, update_modification_date=False)
        cmd.cli_cmd()
        mock_save.assert_called_once_with(
            INSTRUMENT_JSON,
            replace=False,
            update_modification_date=False,
            base_url=INSTRUMENT_BASE_URL,
        )

    @patch("aind_metadata_mapper.cli.instrument.save_instrument")
    def test_upload_exits_on_error(self, mock_save):
        """Upload exits with code 1 when save_instrument raises."""
        mock_save.side_effect = ValueError("Record already exists")
        cmd = Upload(file=INSTRUMENT_JSON)
        with self.assertRaises(SystemExit) as cm:
            cmd.cli_cmd()
        self.assertEqual(cm.exception.code, 1)


class TestGetCmd(unittest.TestCase):
    """Tests for the Get subcommand."""

    @patch("aind_metadata_mapper.cli.instrument.get_instrument")
    def test_get_prints_json_to_stdout(self, mock_get):
        """Get prints instrument JSON to stdout when no output_directory."""
        with open(INSTRUMENT_JSON) as f:
            data = json.load(f)
        mock_get.return_value = instrument.Instrument.model_validate(data)
        cmd = Get(instrument_id="422_MESO2_20241017")
        with patch("builtins.print") as mock_print:
            cmd.cli_cmd()
        mock_get.assert_called_once_with(
            "422_MESO2_20241017",
            modification_date=None,
            output_directory=None,
            base_url=INSTRUMENT_BASE_URL,
        )
        mock_print.assert_called_once()
        # Verify it's valid JSON
        json.loads(mock_print.call_args[0][0])

    @patch("aind_metadata_mapper.cli.instrument.get_instrument")
    def test_get_with_modification_date(self, mock_get):
        """Get passes modification_date to get_instrument."""
        with open(INSTRUMENT_JSON) as f:
            data = json.load(f)
        mock_get.return_value = instrument.Instrument.model_validate(data)
        cmd = Get(instrument_id="422_MESO2_20241017", modification_date="2024-10-28")
        with patch("builtins.print"):
            cmd.cli_cmd()
        mock_get.assert_called_once_with(
            "422_MESO2_20241017",
            modification_date="2024-10-28",
            output_directory=None,
            base_url=INSTRUMENT_BASE_URL,
        )

    @patch("aind_metadata_mapper.cli.instrument.get_instrument")
    def test_get_with_output_directory(self, mock_get):
        """Get passes output_directory and does not print to stdout."""
        with open(INSTRUMENT_JSON) as f:
            data = json.load(f)
        mock_get.return_value = instrument.Instrument.model_validate(data)
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = Get(instrument_id="422_MESO2_20241017", output_directory=Path(tmpdir))
            with patch("builtins.print") as mock_print:
                cmd.cli_cmd()
            mock_get.assert_called_once_with(
                "422_MESO2_20241017",
                modification_date=None,
                output_directory=Path(tmpdir),
                base_url=INSTRUMENT_BASE_URL,
            )
            mock_print.assert_not_called()

    @patch("aind_metadata_mapper.cli.instrument.get_instrument")
    def test_get_exits_when_not_found(self, mock_get):
        """Get exits with code 1 when instrument is not found."""
        mock_get.return_value = None
        cmd = Get(instrument_id="nonexistent")
        with self.assertRaises(SystemExit) as cm:
            cmd.cli_cmd()
        self.assertEqual(cm.exception.code, 1)


class TestInstrumentCLIDispatch(unittest.TestCase):
    """Tests for CLI argument parsing and subcommand dispatch."""

    @patch("aind_metadata_mapper.cli.instrument.save_instrument")
    def test_cli_dispatches_upload(self, mock_save):
        """CliApp.run dispatches upload subcommand from sys.argv."""
        from pydantic_settings import CliApp

        from aind_metadata_mapper.cli.instrument import InstrumentCLI

        cli = CliApp.run(
            InstrumentCLI,
            cli_args=["upload", str(INSTRUMENT_JSON)],
        )
        mock_save.assert_called_once()
        self.assertIsNotNone(cli.upload)

    @patch("aind_metadata_mapper.cli.instrument.CliApp")
    def test_main_calls_cli_app_run(self, mock_cli_app):
        """main() calls CliApp.run with InstrumentCLI."""
        from aind_metadata_mapper.cli.instrument import InstrumentCLI, main

        main()
        mock_cli_app.run.assert_called_once_with(InstrumentCLI)

    @patch("aind_metadata_mapper.cli.instrument.get_instrument")
    def test_cli_dispatches_get(self, mock_get):
        """CliApp.run dispatches get subcommand from sys.argv."""
        with open(INSTRUMENT_JSON) as f:
            data = json.load(f)
        mock_get.return_value = instrument.Instrument.model_validate(data)

        from pydantic_settings import CliApp

        from aind_metadata_mapper.cli.instrument import InstrumentCLI

        with patch("builtins.print"):
            cli = CliApp.run(
                InstrumentCLI,
                cli_args=["get", "422_MESO2_20241017"],
            )
        mock_get.assert_called_once()
        self.assertIsNotNone(cli.get)


if __name__ == "__main__":
    unittest.main()
