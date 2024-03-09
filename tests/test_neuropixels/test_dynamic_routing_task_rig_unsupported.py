import unittest
import pathlib

from aind_metadata_mapper.neuropixels import dynamic_routing_task  # type: ignore

from . import utils as test_utils


class TestDynamicRoutingTaskRigEtl(unittest.TestCase):
    """Tests dxdiag utilities in for the neuropixels project."""

    def test_etl_unsupported(self):
        dynamic_routing_task.logger.setLevel("DEBUG")
        # get expected calibration dates for tests
        for calibration in self.expected.calibrations:
            if calibration.device_name == "Reward delivery":
                expected_water_calibration_date = calibration.calibration_date
                break
        else:
            raise Exception("Water calibration not found")

        etl = dynamic_routing_task.DynamicRoutingTaskRigEtl(
            self.input_source,
            self.output_dir,  # TODO: separate output directory for unsupported
            task_source=pathlib.Path(
                "./tests/resources/neuropixels/"
                "DynamicRouting1_649943_20230213_114903.hdf5"
            ),
            modification_date=self.expected.modification_date,
            reward_calibration_date=expected_water_calibration_date,
        )
        etl.run_job()

        assert self.load_updated() == self.expected

    def setUp(self):
        """Moves required test resources to testing directory.
        """
        # test directory
        self.input_source, self.output_dir, self.expected, self.load_updated, \
                self._cleanup = \
            test_utils.setup_neuropixels_etl_dirs(
                pathlib.Path(
                    "./tests/resources/neuropixels/rig.partial.json"
                ),
                pathlib.Path(
                    "./tests/resources/neuropixels/"
                    "dynamic_routing_task_rig.expected-unsupported.json"
                ),
            )

    def tearDown(self):
        """Removes test resources and directory.
        """
        self._cleanup()