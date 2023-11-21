"""Tests parsing of information related to neuropixels rigs."""

import json
import shutil
import unittest
import tempfile
import pathlib
import typing

from aind_data_schema import rig
from aind_metadata_mapper.neuropixels import rig as neuropixels_rig


def setup_neuropixels_etl_dirs(
        *resources: pathlib.Path
        ) -> tuple[pathlib.Path, pathlib.Path, typing.Callable[[], None]]:
    """Sets up a temporary input/output directory context for neuropixels etl.

    Parameters
    ----------
    resources: paths to etl resources to move to input dir

    Returns
    -------
    input_dir: path to etl input dir
    output_dir: path to etl output dir
    clean_up: cleanup function for input/output dirs
    """
    input_dir = pathlib.Path(tempfile.mkdtemp())
    for resource in resources:
        shutil.copy2(resource, input_dir)

    output_dir = pathlib.Path(tempfile.mkdtemp())

    def clean_up():
        """Clean up callback for temporary directories and their contents."""
        shutil.rmtree(input_dir)
        shutil.rmtree(output_dir)

    return (
        input_dir,
        output_dir,
        clean_up,
    )


class TestNeuropixelsEtl(unittest.TestCase):
    """Tests methods in NeuropixelsEtl class."""

    def test_good_rig_etl(self):
        """Tests that NeuropixelsRigEtl returns the result we expect for valid
        data.
        """
        etl = neuropixels_rig.NeuropixelsRigEtl(
            self.good_input_dir,
            self.good_output_dir
        )
        etl.run_job()
        output = rig.Rig.parse_raw(
            (self.good_output_dir / "rig.json").read_text()
        )
        expected_template = json.loads(
            pathlib.Path("./tests/resources/neuropixels/expected-rig.json")
            .read_text()
        )
        # patch over expected property values that won't stay static over time
        expected = rig.Rig.parse_obj({
            **expected_template,
            "schema_version": output.schema_version,
            "modification_date": output.modification_date,
        })

        assert output == expected

    def test_bad_mvr_rig_etl(self):
        """Tests that the expected exception is raised for an mvr file missing
        an expected mvr camera name.
        """
        etl = neuropixels_rig.NeuropixelsRigEtl(
            self.bad_mvr_input_dir,
            self.bad_mvr_output_dir
        )
        self.assertRaises(
            neuropixels_rig.NeuropixelsRigException,
            etl.run_job,
        )

    def test_bad_mvr_mapping_rig_etl(self):
        """Tests that the expected exception is raised for an mvr mapping file
        with an mvr camera name not present in mvr.ini.
        """
        etl = neuropixels_rig.NeuropixelsRigEtl(
            self.bad_mvr_mapping_input_dir,
            self.bad_mvr_mapping_output_dir
        )
        self.assertRaises(
            neuropixels_rig.NeuropixelsRigException,
            etl.run_job,
        )

    def test_bad_rig_partial_camera_rig_etl(self):
        """Tests that the expected excetion is raised for a partial rig file
         that contains a camera not found in mvr.ini.
        """
        etl = neuropixels_rig.NeuropixelsRigEtl(
            self.bad_rig_partial_camera_input_dir,
            self.bad_rig_partial_camera_output_dir
        )
        self.assertRaises(
            neuropixels_rig.NeuropixelsRigException,
            etl.run_job,
        )

    def test_bad_rig_partial_sync_rig_etl(self):
        """Tests that the expected exception is raised for a partial rig file
         that doesn't contain a sync daq we expect.
        """
        etl = neuropixels_rig.NeuropixelsRigEtl(
            self.bad_rig_partial_sync_input_dir,
            self.bad_rig_partial_sync_output_dir
        )
        self.assertRaises(
            neuropixels_rig.NeuropixelsRigException,
            etl.run_job,
        )

    def test_bad_input_dir_rig_etl(self):
        """Tests that the expected exception is raised when supplying a path
         that isnt a directory.
        """
        etl = neuropixels_rig.NeuropixelsRigEtl(
            pathlib.Path(
                "./tests/resources/neuropixels/good/rig.partial.json"
            ),
            pathlib.Path(
                "./tests/resources/neuropixels/good/rig.partial.json"
            ),
        )
        self.assertRaises(
            neuropixels_rig.NeuropixelsRigException,
            etl.run_job,
        )

    def test_missing_sync_rig_etl(self):
        """Tests that the expected exception is raised when the sync file is
        missing.
        """
        etl = neuropixels_rig.NeuropixelsRigEtl(
            self.missing_sync_input_dir,
            self.missing_sync_output_dir
        )
        self.assertRaises(
            neuropixels_rig.NeuropixelsRigException,
            etl.run_job,
        )

    def setUp(self):
        """Moves required test resources to testing directory.
        """
        good_rig_partial_path = pathlib.Path(
            "./tests/resources/neuropixels/good/rig.partial.json"
        )
        good_mvr_path = pathlib.Path(
            "./tests/resources/neuropixels/good/mvr.ini"
        )
        good_mvr_mapping_path = pathlib.Path(
            "./tests/resources/neuropixels/good/mvr.mapping.json"
        )
        good_sync_path = pathlib.Path(
            "./tests/resources/neuropixels/good/sync.yml"
        )

        # setup good directory
        self.good_input_dir, self.good_output_dir, self._cleanup_good = \
            setup_neuropixels_etl_dirs(
                good_rig_partial_path,
                good_mvr_path,
                good_mvr_mapping_path,
                good_sync_path,
            )

        # setup bad mvr directory
        bad_mvr_path = pathlib.Path(
            "./tests/resources/neuropixels/bad/mvr.ini"
        )
        self.bad_mvr_input_dir, \
            self.bad_mvr_output_dir, self._cleanup_bad_mvr = \
            setup_neuropixels_etl_dirs(
                good_rig_partial_path,
                bad_mvr_path,
                good_mvr_mapping_path,
                good_sync_path,
            )

        # setup bad mvr mapping directory
        bad_mvr_mapping_path = pathlib.Path(
            "./tests/resources/neuropixels/bad/mvr.mapping.json"
        )
        self.bad_mvr_mapping_input_dir, \
            self.bad_mvr_mapping_output_dir, self._cleanup_bad_mvr_mapping = \
            setup_neuropixels_etl_dirs(
                good_rig_partial_path,
                good_mvr_path,
                bad_mvr_mapping_path,
                good_sync_path,
            )

        # setup bad rig partial camera directory
        bad_rig_partial_camera_path = pathlib.Path(
            "./tests/resources/neuropixels/bad/rig-camera/rig.partial.json"
        )
        self.bad_rig_partial_camera_input_dir, \
            self.bad_rig_partial_camera_output_dir, \
            self._cleanup_bad_rig_partial_camera = \
            setup_neuropixels_etl_dirs(
                bad_rig_partial_camera_path,
                good_mvr_path,
                good_mvr_mapping_path,
                good_sync_path,
            )

        # setup bad rig partial sync directory
        bad_rig_partial_sync_path = pathlib.Path(
            "./tests/resources/neuropixels/bad/rig-sync/rig.partial.json"
        )
        self.bad_rig_partial_sync_input_dir, \
            self.bad_rig_partial_sync_output_dir, \
            self._cleanup_bad_rig_partial_sync = \
            setup_neuropixels_etl_dirs(
                bad_rig_partial_sync_path,
                good_mvr_path,
                good_mvr_mapping_path,
                good_sync_path,
            )

        # setup bad rig directory missing required files
        self.missing_sync_input_dir, \
            self.missing_sync_output_dir, \
            self._cleanup_missing_sync = \
            setup_neuropixels_etl_dirs(
                good_rig_partial_path,
                good_mvr_path,
                good_mvr_mapping_path,
            )

    def tearDown(self):
        """Removes test resources and directory.
        """
        self._cleanup_good()
        self._cleanup_bad_mvr()
        self._cleanup_bad_mvr_mapping()
        self._cleanup_bad_rig_partial_camera()
        self._cleanup_bad_rig_partial_sync()
        self._cleanup_missing_sync()


if __name__ == "__main__":
    unittest.main()
