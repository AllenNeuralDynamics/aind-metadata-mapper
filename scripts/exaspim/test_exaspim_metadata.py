# flake8: noqa: C901
"""Integration test for the exaSPIM metadata mapper.

Copies ``acquisition.json`` and ``instrument.json`` from a real exaSPIM
dataset directory into a temp directory, then runs
:class:`~aind_metadata_mapper.exaspim.mapper.ExaSPIMMapper` to upgrade
v1 metadata to v2.  No metadata-service calls are made — this purely
exercises the mapper / upgrader pipeline.

Usage
-----
.. code-block:: bash

   python scripts/exaspim/test_exaspim_metadata.py \\
       --dataset-dir /allen/aind/stage/exaspim/exaSPIM_826507_2026-05-29_16-56-55
"""

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

from packaging import version as pkg_version

from aind_metadata_mapper.base import MapperJobSettings
from aind_metadata_mapper.exaspim.mapper import ExaSPIMMapper

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DEFAULT_DATASET_DIR = "/allen/aind/stage/exaspim/1x_screening/" "exaSPIM_821805_2026-05-11_16-32-29"


def _print_header(title: str) -> None:
    """Print a section header."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _load(path: Path) -> dict:
    """Load a JSON file and return the parsed dict."""
    with open(path) as fh:
        return json.load(fh)


def main() -> int:
    """Run the exaSPIM integration test.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on failure.
    """
    parser = argparse.ArgumentParser(
        description="Integration test for the exaSPIM metadata mapper.",
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=Path(DEFAULT_DATASET_DIR),
        help=(
            "Path to a real exaSPIM dataset directory containing "
            "v1 acquisition.json (and optionally instrument.json). "
            f"Default: {DEFAULT_DATASET_DIR}"
        ),
    )
    args = parser.parse_args()
    dataset_dir: Path = args.dataset_dir

    # ------------------------------------------------------------------
    # Validate source directory
    # ------------------------------------------------------------------
    _print_header("INTEGRATION TEST: ExaSPIM Metadata Mapper")
    print(f"Dataset directory: {dataset_dir}")

    if not dataset_dir.is_dir():
        print(f"\n✗ ERROR: directory does not exist: {dataset_dir}")
        return 1

    src_acq = dataset_dir / "acquisition.json"
    src_inst = dataset_dir / "instrument.json"
    src_inst_yaml = dataset_dir / "derivatives" / "instrument_config.yaml"

    if not src_acq.is_file():
        print(f"\n✗ ERROR: acquisition.json not found in {dataset_dir}")
        return 1

    has_instrument = src_inst.is_file()
    has_inst_yaml = src_inst_yaml.is_file()
    print(f"  acquisition.json           : found")
    if has_instrument:
        print(f"  instrument.json            : found")
    elif has_inst_yaml:
        print(f"  instrument.json            : NOT FOUND")
        print(f"  derivatives/instrument_config.yaml : found (detection fallback)")
    else:
        print(f"  instrument.json            : NOT FOUND (will use acq-only upgrade)")

    # ------------------------------------------------------------------
    # Copy files to a temp directory
    # ------------------------------------------------------------------
    temp_dir = tempfile.mkdtemp(prefix="exaspim_integration_")
    tmp_path = Path(temp_dir)

    try:
        print(f"\nTemporary directory: {tmp_path}")
        shutil.copy(src_acq, tmp_path / "acquisition.json")
        os.chmod(tmp_path / "acquisition.json", 0o644)
        if has_instrument:
            shutil.copy(src_inst, tmp_path / "instrument.json")
            os.chmod(tmp_path / "instrument.json", 0o644)
        if has_inst_yaml:
            deriv = tmp_path / "derivatives"
            deriv.mkdir(exist_ok=True)
            shutil.copy(src_inst_yaml, deriv / "instrument_config.yaml")
            os.chmod(deriv / "instrument_config.yaml", 0o644)
        print("✓ Files copied (permissions fixed to 0644)")

        # ------------------------------------------------------------------
        # Phase 1 — Detection
        # ------------------------------------------------------------------
        _print_header("PHASE 1: Detection")

        detected = ExaSPIMMapper.detect(tmp_path)
        if detected:
            print("✓ ExaSPIMMapper.detect() → True")
        else:
            print("✗ ExaSPIMMapper.detect() → False")
            if not has_instrument:
                print("  (Expected — no instrument.json for detection. " "Continuing with direct upgrade.)")
            else:
                print(
                    "  WARNING: instrument.json is present but "
                    "detection failed. The upgrade will still be "
                    "attempted."
                )

        # ------------------------------------------------------------------
        # Phase 2 — Pre-upgrade inspection
        # ------------------------------------------------------------------
        _print_header("PHASE 2: Pre-Upgrade Inspection")

        acq_before = _load(tmp_path / "acquisition.json")
        acq_sv_before = acq_before.get("schema_version", "unknown")
        print(f"  acquisition.json schema_version : {acq_sv_before}")
        print(f"  subject_id                      : {acq_before.get('subject_id')}")
        print(f"  instrument_id                   : {acq_before.get('instrument_id')}")
        print(f"  tiles                           : {len(acq_before.get('tiles', []))}")

        inst_sv_before = None
        if has_instrument:
            inst_before = _load(tmp_path / "instrument.json")
            inst_sv_before = inst_before.get("schema_version", "unknown")
            print(f"  instrument.json schema_version  : {inst_sv_before}")
            print(f"  instrument_type                 : {inst_before.get('instrument_type')}")

        # ------------------------------------------------------------------
        # Phase 3 — Upgrade
        # ------------------------------------------------------------------
        _print_header("PHASE 3: Upgrade")

        mapper = ExaSPIMMapper()
        job_settings = MapperJobSettings(
            input_filepath=tmp_path / "acquisition.json",
            output_directory=tmp_path,
            output_filename_suffix="exaspim",
        )

        print("Running ExaSPIMMapper.run_job() …")
        try:
            mapper.run_job(job_settings)
            print("✓ Upgrade completed successfully")
        except Exception as exc:
            print(f"✗ Upgrade FAILED: {exc}")
            return 1

        # ------------------------------------------------------------------
        # Phase 4 — Post-upgrade validation
        # ------------------------------------------------------------------
        _print_header("PHASE 4: Post-Upgrade Validation")

        acq_after = _load(tmp_path / "acquisition.json")
        acq_sv_after = acq_after.get("schema_version", "unknown")
        acq_ok = pkg_version.parse(acq_sv_after) >= pkg_version.parse("2.0.0")
        has_streams = "data_streams" in acq_after
        n_streams = len(acq_after.get("data_streams", []))

        print(f"  acquisition.json schema_version : {acq_sv_before} → {acq_sv_after}")
        print(f"  schema_version >= 2.0.0         : {'✓' if acq_ok else '✗'}")
        print(f"  data_streams present            : {'✓' if has_streams else '✗'} ({n_streams} streams)")

        inst_ok = True
        if has_instrument and (tmp_path / "instrument.json").is_file():
            inst_after = _load(tmp_path / "instrument.json")
            inst_sv_after = inst_after.get("schema_version", "unknown")
            inst_ok = pkg_version.parse(inst_sv_after) >= pkg_version.parse("2.0.0")
            print(f"  instrument.json schema_version  : {inst_sv_before} → {inst_sv_after}")
            print(f"  schema_version >= 2.0.0         : {'✓' if inst_ok else '✗'}")

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        _print_header("SUMMARY")

        all_passed = acq_ok and has_streams and inst_ok
        print(f"  Detection          : {'✓' if detected else '⚠ (not critical)'}")
        print(f"  Acquisition upgrade: {'✓' if acq_ok else '✗'}")
        print(f"  data_streams       : {'✓' if has_streams else '✗'}")
        if has_instrument:
            print(f"  Instrument upgrade : {'✓' if inst_ok else '✗'}")
        print()
        print(f"  Overall: {'✓ PASSED' if all_passed else '✗ FAILED'}")
        print("=" * 80 + "\n")

        return 0 if all_passed else 1

    finally:
        print(f"Cleaning up temporary directory: {tmp_path}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("✓ Cleanup complete\n")


if __name__ == "__main__":
    sys.exit(main())
