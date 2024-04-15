"""Utilities for neuropixels etl tests."""
import os
from pathlib import Path
from typing import Tuple

from aind_data_schema.core.rig import Rig  # type: ignore

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "neuropixels"
)

FORWARD_CAMERA_ASSEMBLY_NAME = "Forward"
FORWARD_CAMERA_NAME = f"{FORWARD_CAMERA_ASSEMBLY_NAME} camera"
EYE_CAMERA_ASSEMBLY_NAME = "Eye"
EYE_CAMERA_NAME = f"{EYE_CAMERA_ASSEMBLY_NAME} camera"
SIDE_CAMERA_ASSEMBLY_NAME = "Side"
SIDE_CAMERA_NAME = f"{SIDE_CAMERA_ASSEMBLY_NAME} camera"


def setup_neuropixels_etl_resources(
    expected_json: Path,
) -> Tuple[Path, Path, Rig]:
    """Sets test resources neuropixels etl.

    Parameters
    ----------
    expected_json: Path
      paths to etl resources to move to input dir

    Returns
    -------
    Tuple[Path, Path, Rig]
      input_source: path to etl base rig input source
      output_dir: path to etl output directory
      expected_rig: rig model to compare to output
    """
    return (
        RESOURCES_DIR / "base_rig.json",
        Path("./"),  # hopefully file writes are mocked
        Rig.model_validate_json(expected_json.read_text()),
    )
