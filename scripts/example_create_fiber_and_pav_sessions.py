"""
Create a unified session metadata file by generating and merging
Pavlovian behavior and fiber photometry metadata.

This script serves as a single entry point for:
1. Generating Pavlovian behavior session metadata
2. Generating fiber photometry session metadata
3. Merging the two session files into a unified metadata file

Example Usage:
    To create a unified session metadata file from the command line:

    ```bash
    python scripts/example_create_fiber_and_pav_sessions.py \
        --subject-id "000000" \
        --data-dir data/sample_fiber_data \
        --output-dir data/sample_fiber_data \
        --experimenters "Test User 1" "Test User 2" \
        --session-type "Pavlovian_Conditioning + FIB" \
        --behavior-output "session_pavlovian.json" \
        --fiber-output "session_fib.json" \
        --merged-output "session.json"
    ```

    This will:
    1. Generate Pavlovian behavior metadata in 'pav_behavior.json'
    2. Generate fiber photometry metadata in 'fiber_phot.json'
    3. Merge both files into a unified 'session_combined.json'

    All optional parameters (rig_id, iacuc, notes, etc.)
    will use default values unless specified.
    See --help for full list of options.
"""

import argparse
import sys
import logging
from pathlib import Path

from aind_metadata_mapper.pavlovian_behavior.example_create_session import (
    create_metadata as create_pavlovian_metadata,
)
from aind_metadata_mapper.fip.example_create_session import (
    create_metadata as create_fip_metadata,
)
from aind_metadata_mapper.utils.merge_sessions import merge_sessions
from aind_data_schema.core.session import Session


def create_unified_session_metadata(
    subject_id: str,
    data_dir: Path | str,
    output_dir: Path | str = Path.cwd(),
    experimenters: list[str] = (),
    *,
    rig_id: str | None = None,
    iacuc: str | None = None,
    notes: str | None = None,
    reward_volume: float | None = None,
    reward_unit: str | None = None,
    session_type: str | None = None,
    behavior_output: str = "session_pavlovian.json",
    fiber_output: str = "session_fib.json",
    merged_output: str = "session.json",
    active_mouse_platform: bool = False,
    local_timezone: str = "America/Los_Angeles",
    anaesthesia: str | None = None,
    animal_weight_post: float | None = None,
    animal_weight_prior: float | None = None,
    mouse_platform_name: str = "mouse_tube_foraging",
) -> Path:
    """Generate Pavlovian behavior metadata, fiber photometry metadata,
    merge them into a unified session file, and return its path.

    Parameters
    ----------
    subject_id : str
        Unique identifier for the experimental subject
    data_dir : Path | str
        Root directory containing 'behavior' and 'fib' subdirectories
    output_dir : Path | str, optional
        Directory where metadata files will be saved, by default Path.cwd()
    experimenters : list[str], optional
        List of experimenter full names, by default ()
    rig_id : str | None, optional
        Identifier for the experimental rig, by default None
    iacuc : str | None, optional
        IACUC protocol identifier, by default None
    notes : str | None, optional
        Additional notes about the session, by default None
    reward_volume : float | None, optional
        Volume of reward delivered per successful trial, by default None
    reward_unit : str | None, optional
        Unit of reward volume, by default None
    session_type : str | None, optional
        Session type to use for both behavior and fiber metadata,
        by default None
    behavior_output : str, optional
        Filename for behavior session metadata,
        by default "session_pavlovian.json"
    fiber_output : str, optional
        Filename for fiber photometry session metadata,
        by default "session_fib.json"
    merged_output : str, optional
        Filename for merged session metadata, by default "session.json"
    active_mouse_platform : bool, optional
        Whether the mouse platform was active, by default False
    local_timezone : str, optional
        Local timezone for the session, by default "America/Los_Angeles"
    anaesthesia : str | None, optional
        Anaesthesia used, by default None
    animal_weight_post : float | None, optional
        Animal weight after session, by default None
    animal_weight_prior : float | None, optional
        Animal weight before session, by default None
    mouse_platform_name : str, optional
        Name of the mouse platform, by default "mouse_tube_foraging"

    Returns
    -------
    Path
        Path to the generated unified session metadata file

    Raises
    ------
    RuntimeError
        If either Pavlovian behavior or
        fiber photometry metadata generation fails
    """
    # Ensure paths exist
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build and filter kwargs for Pavlovian behavior metadata generation
    pav_kwargs = {
        "subject_id": subject_id,
        "data_directory": data_dir,
        "output_directory": output_dir,
        "output_filename": behavior_output,
        "experimenter_full_name": experimenters,
        "rig_id": rig_id,
        "iacuc_protocol": iacuc,
        "notes": notes,
        "reward_units_per_trial": reward_volume,
        "reward_consumed_unit": reward_unit,
        "session_type": session_type,
        "active_mouse_platform": active_mouse_platform,
        "local_timezone": local_timezone,
        "anaesthesia": anaesthesia,
        "animal_weight_post": animal_weight_post,
        "animal_weight_prior": animal_weight_prior,
        "mouse_platform_name": mouse_platform_name,
    }
    pav_kwargs = {k: v for k, v in pav_kwargs.items() if v is not None}

    # Run Pavlovian behavior ETL
    logging.info("Generating Pavlovian behavior metadata…")
    if not create_pavlovian_metadata(**pav_kwargs):
        raise RuntimeError("Failed to generate Pavlovian behavior metadata")

    # Build and filter kwargs for fiber photometry metadata generation
    fip_kwargs = {
        "subject_id": subject_id,
        "data_directory": data_dir,
        "output_directory": output_dir,
        "output_filename": fiber_output,
        "experimenter_full_name": experimenters,
        "rig_id": rig_id,
        "iacuc_protocol": iacuc,
        "notes": notes,
        "session_type": session_type,
        "active_mouse_platform": active_mouse_platform,
        "local_timezone": local_timezone,
        "anaesthesia": anaesthesia,
        "animal_weight_post": animal_weight_post,
        "animal_weight_prior": animal_weight_prior,
        "mouse_platform_name": mouse_platform_name,
    }
    fip_kwargs = {k: v for k, v in fip_kwargs.items() if v is not None}

    # Run fiber photometry ETL
    logging.info("Generating fiber photometry metadata…")
    if not create_fip_metadata(**fip_kwargs):
        raise RuntimeError("Failed to generate fiber photometry metadata")

    # Merge the two session files into one
    logging.info("Merging session metadata files…")
    merged = merge_sessions(
        session_file1=output_dir / behavior_output,
        session_file2=output_dir / fiber_output,
        output_file=output_dir / merged_output,
    )

    # Validate via pydantic and write final JSON
    session_model = Session(**merged)
    merged_path = output_dir / merged_output
    with open(merged_path, "w") as f:
        f.write(session_model.model_dump_json(indent=2))

    logging.info(f"Unified session metadata created at: {merged_path}")
    return merged_path


def main():
    """Parse command line arguments and create unified session metadata.

    This function:
    1. Sets up argument parsing for all required and optional parameters
    2. Calls create_unified_session_metadata with the parsed arguments
    3. Handles any exceptions and exits with status code 1 if an error occurs

    Notes
    -----
    The script requires both behavior and fiber data directories to be present
    under the specified data directory. The output will be a unified session
    metadata file that combines information from both data types.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Create unified session metadata from behavior and fiber data"
        )
    )
    parser.add_argument(
        "--subject-id",
        type=str,
        required=True,
        help="Subject identifier",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Root directory containing 'behavior' and 'fib' subdirectories",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        help=(
            "Directory where metadata files will be saved "
            "(default: current directory)"
        ),
    )
    parser.add_argument(
        "--experimenters",
        type=str,
        nargs="+",
        required=True,
        help="List of experimenter full names",
    )
    parser.add_argument(
        "--rig-id",
        type=str,
        default=None,
        help="Identifier for the experimental rig",
    )
    parser.add_argument(
        "--iacuc",
        type=str,
        default=None,
        help="IACUC protocol identifier",
    )
    parser.add_argument(
        "--notes",
        type=str,
        default=None,
        help="Additional notes about the session",
    )
    parser.add_argument(
        "--reward-volume",
        type=float,
        default=None,
        help="Volume of reward delivered per successful trial",
    )
    parser.add_argument(
        "--reward-unit",
        type=str,
        choices=["microliter", "milliliter"],
        default=None,
        help="Unit of reward volume",
    )
    parser.add_argument(
        "--session-type",
        type=str,
        default=None,
        help="Session type to use for both behavior and fiber metadata "
        "(overrides individual defaults if specified)",
    )
    parser.add_argument(
        "--behavior-output",
        type=str,
        default="session_pavlovian.json",
        help="Filename for behavior session metadata "
        "(default: session_pavlovian.json)",
    )
    parser.add_argument(
        "--fiber-output",
        type=str,
        default="session_fib.json",
        help="Filename for fiber photometry session metadata "
        "(default: session_fib.json)",
    )
    parser.add_argument(
        "--merged-output",
        type=str,
        default="session.json",
        help="Filename for merged session metadata (default: session.json)",
    )
    parser.add_argument(
        "--active-mouse-platform",
        action="store_true",
        help="Whether the mouse platform was active",
    )
    parser.add_argument(
        "--local-timezone",
        type=str,
        default=None,
        help="Local timezone for the session",
    )
    parser.add_argument(
        "--anaesthesia",
        type=str,
        default=None,
        help="Anaesthesia used",
    )
    parser.add_argument(
        "--animal-weight-post",
        type=float,
        default=None,
        help="Animal weight after session",
    )
    parser.add_argument(
        "--animal-weight-prior",
        type=float,
        default=None,
        help="Animal weight before session",
    )
    parser.add_argument(
        "--mouse-platform-name",
        type=str,
        default="mouse_tube_foraging",
        help="Name of the mouse platform",
    )

    args = parser.parse_args()

    try:
        create_unified_session_metadata(
            subject_id=args.subject_id,
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            experimenters=args.experimenters,
            rig_id=args.rig_id,
            iacuc=args.iacuc,
            notes=args.notes,
            reward_volume=args.reward_volume,
            reward_unit=args.reward_unit,
            session_type=args.session_type,
            behavior_output=args.behavior_output,
            fiber_output=args.fiber_output,
            merged_output=args.merged_output,
            active_mouse_platform=args.active_mouse_platform,
            local_timezone=args.local_timezone,
            anaesthesia=args.anaesthesia,
            animal_weight_post=args.animal_weight_post,
            animal_weight_prior=args.animal_weight_prior,
            mouse_platform_name=args.mouse_platform_name,
        )
    except Exception as e:
        logging.error(e)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="\n%(asctime)s - %(message)s\n",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
