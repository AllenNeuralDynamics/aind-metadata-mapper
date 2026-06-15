"""exaSPIM metadata pre-processor.

Detects exaSPIM datasets and upgrades v1 ``acquisition.json`` /
``instrument.json`` to aind-data-schema v2 using
``aind-metadata-upgrader``.  Upgraded files are written back **in
place** in the metadata directory; S3 upload is the caller's
responsibility.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Optional

import yaml
from packaging import version as pkg_version

from aind_metadata_mapper.base import MapperJob, MapperJobSettings
from aind_metadata_mapper.exaspim.constants import (
    AXIS_DIRECTION_MAP,
    DEPRECATED_MOTORIZED_FIELDS,
    DEVICE_LISTS,
    EXASPIM_INSTRUMENT_KEYWORDS,
    FILTER_TYPE_MAP,
    INSTRUMENT_ID_MAP,
    V2_THRESHOLD,
    VALID_AXIS_DIRECTIONS,
    VALID_STAGE_AXIS_NAMES,
    VALID_TRAVEL_UNITS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_json(path: Path) -> Optional[dict]:
    """Load a JSON file, returning ``None`` if missing or empty."""
    if not path.is_file():
        return None
    try:
        with open(path, "r") as fh:
            data = json.load(fh)
        return data if data else None
    except (json.JSONDecodeError, OSError):
        logger.warning("Could not read %s", path)
        return None


def _write_json(path: Path, data: dict) -> None:
    """Write *data* as pretty-printed JSON to *path*."""
    with open(path, "w") as fh:
        json.dump(data, fh, indent=3, default=str, ensure_ascii=False)


def _needs_upgrade(data: dict) -> bool:
    """Return ``True`` when ``schema_version`` is below ``V2_THRESHOLD``."""
    sv = data.get("schema_version", "0.0.0")
    return pkg_version.parse(sv) < pkg_version.parse(V2_THRESHOLD)


def _to_json_dict(data: Any) -> Optional[dict]:
    """Convert an upgrader output to a plain ``dict``."""
    if data is None:
        return None
    if hasattr(data, "model_dump"):
        return data.model_dump(mode="json", exclude_none=True)
    return data  # pragma: no cover


def _contains_exaspim_keyword(text: str) -> bool:
    """Case-insensitive check for any exaSPIM keyword in *text*."""
    lower = text.lower()
    return any(kw in lower for kw in EXASPIM_INSTRUMENT_KEYWORDS)


def _resolve_instrument_from_yaml(metadata_dir: Path) -> Optional[dict]:
    """Resolve a reference instrument JSON from ``instrument_config.yaml``.

    Reads ``derivatives/instrument_config.yaml``, extracts the
    ``instrument.id`` field, maps it via :data:`INSTRUMENT_ID_MAP`,
    and loads the corresponding bundled reference JSON.

    Parameters
    ----------
    metadata_dir : Path
        Directory containing the dataset (with a ``derivatives/``
        subdirectory).

    Returns
    -------
    dict or None
        Parsed instrument dict if resolved, else ``None``.
    """
    yaml_path = metadata_dir / "derivatives" / "instrument_config.yaml"
    if not yaml_path.is_file():
        return None

    try:
        with open(yaml_path, "r") as fh:
            content = yaml.safe_load(fh)
    except Exception:
        logger.warning(
            "Could not parse %s for instrument resolution",
            yaml_path,
        )
        return None

    if not isinstance(content, dict):
        return None

    instrument_id = (
        content.get("instrument", {}).get("id", "")
    )
    if not instrument_id:
        logger.debug(
            "No instrument.id found in %s", yaml_path
        )
        return None

    filename = INSTRUMENT_ID_MAP.get(instrument_id)
    if filename is None:
        logger.warning(
            "Unknown instrument id %r in %s — no reference "
            "instrument available.",
            instrument_id,
            yaml_path,
        )
        return None

    instruments_dir = Path(__file__).parent / "instruments"
    ref_path = instruments_dir / filename
    if not ref_path.is_file():
        logger.error(
            "Reference instrument file missing: %s", ref_path
        )
        return None

    return _load_json(ref_path)


# ---------------------------------------------------------------------------
# Instrument sanitisation (5-step pre-processing)
# ---------------------------------------------------------------------------


def sanitize_instrument(inst_data: dict) -> dict:
    """Pre-process a v1 instrument dict before upgrading to v2.

    Applies five fixes that prevent ``aind-metadata-upgrader`` from
    crashing on real-world exaSPIM instruments:

    1. Replace unrecognised manufacturer names with ``"Other"``.
    2. Default missing ``magnification`` on objectives to ``1.0``.
    3. Parse ``center_wavelength`` on multiband filters from the model
       string.
    4. Strip deprecated fields from motorised stages and fix invalid
       ``travel_unit``.
    5. Normalise ``stage_axis_direction`` on scanning stages.

    All changes are documented in the device ``notes`` field with a
    ``(v1v2 pre-process):`` prefix.

    Parameters
    ----------
    inst_data : dict
        Raw v1 instrument dictionary.

    Returns
    -------
    dict
        Sanitised deep copy of the instrument dictionary.
    """
    from aind_data_schema_models.organizations import Organization

    inst: dict = json.loads(json.dumps(inst_data))  # deep copy

    # 1. Unrecognised manufacturers → "Other"
    for key in DEVICE_LISTS:
        for device in inst.get(key) or []:
            mfr = device.get("manufacturer")
            if not isinstance(mfr, dict):
                continue
            name = mfr.get("name", "")
            if name and Organization.from_name(name) is None:
                logger.warning(
                    "Manufacturer %r not recognised — replacing with "
                    "'Other' in %s[%s]",
                    name,
                    key,
                    device.get("name", "?"),
                )
                _append_note(
                    device,
                    f"original manufacturer was '{name}'",
                )
                mfr["name"] = "Other"

    # 2. Missing magnification on objectives
    for objective in inst.get("objectives") or []:
        if (
            "magnification" not in objective
            or objective["magnification"] is None
        ):
            logger.warning(
                "Objective %r missing 'magnification' — "
                "defaulting to 1.0",
                objective.get("name", "?"),
            )
            _append_note(
                objective,
                "magnification was missing, defaulted to 1.0",
            )
            objective["magnification"] = 1.0

    # 3. Multiband filter center_wavelength
    for filt in inst.get("fluorescence_filters") or []:
        if (
            filt.get("filter_type") == "Multiband"
            and not filt.get("center_wavelength")
        ):
            model = filt.get("model", "")
            wavelengths = [int(m) for m in re.findall(r"\d{3}", model)]
            if wavelengths:
                logger.warning(
                    "Multiband filter %r (model %r) — parsed "
                    "center_wavelength %s from model name",
                    filt.get("name", "?"),
                    model,
                    wavelengths,
                )
                filt["center_wavelength"] = wavelengths
            else:
                logger.warning(
                    "Multiband filter %r (model %r) — could not "
                    "parse center_wavelength; upgrade may fail",
                    filt.get("name", "?"),
                    model,
                )

    # 3b. Normalise legacy filter_type strings
    for filt in inst.get("fluorescence_filters") or []:
        ft = filt.get("filter_type", "")
        if ft and ft in FILTER_TYPE_MAP:
            logger.warning(
                "Filter %r: remapping filter_type %r → %r",
                filt.get("name", "?"),
                ft,
                FILTER_TYPE_MAP[ft],
            )
            filt["filter_type"] = FILTER_TYPE_MAP[ft]

    # 4. Deprecated motorised-stage fields + invalid travel_unit
    for stage in inst.get("motorized_stages") or []:
        removed: dict[str, Any] = {}
        for field in DEPRECATED_MOTORIZED_FIELDS:
            if field in stage:
                removed[field] = stage.pop(field)
        if removed:
            logger.warning(
                "Removed deprecated fields %s from "
                "motorized_stages[%s]",
                list(removed.keys()),
                stage.get("name", "?"),
            )
            for k, v in removed.items():
                _append_note(stage, f"removed {k}={v!r}")

        tu = stage.get("travel_unit")
        if tu and tu not in VALID_TRAVEL_UNITS:
            logger.warning(
                "Stage %r has invalid travel_unit %r — "
                "remapping to 'millimeter'",
                stage.get("name", "?"),
                tu,
            )
            _append_note(stage, f"original travel_unit was '{tu}'")
            stage["travel_unit"] = "millimeter"

    # 5. Normalise scanning-stage axis direction + travel_unit
    for stage in inst.get("scanning_stages") or []:
        # Fix invalid travel_unit on scanning stages too
        tu = stage.get("travel_unit")
        if tu and tu not in VALID_TRAVEL_UNITS:
            logger.warning(
                "Scanning stage %r has invalid travel_unit %r — "
                "remapping to 'millimeter'",
                stage.get("name", "?"),
                tu,
            )
            _append_note(stage, f"original travel_unit was '{tu}'")
            stage["travel_unit"] = "millimeter"

        sad = stage.get("stage_axis_direction", "")
        if not sad:
            logger.warning(
                "Scanning stage %r: missing "
                "stage_axis_direction — defaulting to "
                "'Detection axis'",
                stage.get("name", "?"),
            )
            _append_note(
                stage,
                "stage_axis_direction was missing, "
                "defaulted to 'Detection axis'",
            )
            stage["stage_axis_direction"] = "Detection axis"
        elif sad not in VALID_AXIS_DIRECTIONS:
            mapped = None
            sad_lower = sad.lower()
            for keyword, canonical in AXIS_DIRECTION_MAP.items():
                if keyword in sad_lower:
                    mapped = canonical
                    break
            if mapped:
                logger.warning(
                    "Scanning stage %r: remapping "
                    "stage_axis_direction %r → %r",
                    stage.get("name", "?"),
                    sad,
                    mapped,
                )
                _append_note(
                    stage,
                    f"original stage_axis_direction was '{sad}'",
                )
                stage["stage_axis_direction"] = mapped
            else:
                logger.warning(
                    "Scanning stage %r: unknown "
                    "stage_axis_direction %r — defaulting to "
                    "'Detection axis'",
                    stage.get("name", "?"),
                    sad,
                )
                stage["stage_axis_direction"] = "Detection axis"

    # 5b. Normalise scanning-stage axis name
    for stage in inst.get("scanning_stages") or []:
        san = stage.get("stage_axis_name", "")
        if not san:
            logger.warning(
                "Scanning stage %r: missing stage_axis_name "
                "— defaulting to 'X'",
                stage.get("name", "?"),
            )
            _append_note(
                stage,
                "stage_axis_name was missing, defaulted to 'X'",
            )
            stage["stage_axis_name"] = "X"
        elif san not in VALID_STAGE_AXIS_NAMES:
            logger.warning(
                "Scanning stage %r: invalid stage_axis_name %r "
                "— defaulting to 'X'",
                stage.get("name", "?"),
                san,
            )
            _append_note(
                stage,
                f"original stage_axis_name was '{san}', "
                f"defaulted to 'X'",
            )
            stage["stage_axis_name"] = "X"

    return inst


def _append_note(device: dict, message: str) -> None:
    """Append a ``(v1v2 pre-process):`` note to *device*."""
    note = f"(v1v2 pre-process): {message}"
    existing = device.get("notes") or ""
    device["notes"] = (
        f"{existing} {note}".strip() if existing else note
    )


# ---------------------------------------------------------------------------
# Upgrade helpers
# ---------------------------------------------------------------------------


def upgrade_with_instrument(
    acq_data: dict, inst_data: dict
) -> tuple[dict, Optional[dict]]:
    """Upgrade both acquisition and instrument via ``Upgrade()``.

    Parameters
    ----------
    acq_data : dict
        Raw v1 acquisition dictionary.
    inst_data : dict
        Raw v1 instrument dictionary.

    Returns
    -------
    tuple[dict, dict | None]
        ``(upgraded_acq, upgraded_inst)`` as plain dicts.
    """
    from aind_metadata_upgrader.upgrade import Upgrade

    sanitized_inst = sanitize_instrument(inst_data)
    record: dict = {"acquisition": acq_data, "instrument": sanitized_inst}
    upgraded = Upgrade(record, skip_metadata_validation=True)

    upgraded_acq = _to_json_dict(upgraded.metadata.acquisition)
    upgraded_inst = _to_json_dict(upgraded.metadata.instrument)
    return upgraded_acq, upgraded_inst


def upgrade_acquisition_only(acq_data: dict) -> dict:
    """Upgrade acquisition without instrument metadata.

    Uses an empty filter / light-source stub so the upgrader can
    proceed without a valid instrument.

    Parameters
    ----------
    acq_data : dict
        Raw v1 acquisition dictionary.

    Returns
    -------
    dict
        Upgraded acquisition as a plain dict.
    """
    from aind_data_schema.core.acquisition import Acquisition
    from aind_metadata_upgrader.acquisition.v1v2 import AcquisitionV1V2

    target_version = Acquisition.model_fields["schema_version"].default

    metadata_stub: dict = {
        "instrument": {
            "fluorescence_filters": [],
            "light_sources": [],
        },
    }

    upgraded_data = AcquisitionV1V2().upgrade(
        acq_data.copy(), target_version, metadata=metadata_stub
    )

    acq_model = Acquisition.model_construct(**upgraded_data)
    return acq_model.model_dump(mode="json", exclude_none=True)


# ---------------------------------------------------------------------------
# Pre-processor
# ---------------------------------------------------------------------------


class ExaSPIMMapper(MapperJob):
    """Mapper for exaSPIM datasets.

    Detects exaSPIM instrument metadata and upgrades v1 files to v2
    using ``aind-metadata-upgrader``.
    """

    @classmethod
    def detect(cls, metadata_dir: Path) -> bool:
        """Return ``True`` if *metadata_dir* contains exaSPIM metadata.

        Checks ``instrument.json`` fields ``instrument_id`` and
        ``instrument_type``.  Falls back to scanning
        ``derivatives/instrument_config.yaml`` for an exaSPIM keyword.

        Parameters
        ----------
        metadata_dir : Path
            Directory containing the raw metadata files.

        Returns
        -------
        bool
        """
        # Primary: instrument.json
        inst_path = metadata_dir / "instrument.json"
        inst_data = _load_json(inst_path)
        if inst_data is not None:
            for field in ("instrument_id", "instrument_type"):
                value = inst_data.get(field, "")
                if isinstance(value, str) and _contains_exaspim_keyword(
                    value
                ):
                    return True

        # Fall-back: derivatives/instrument_config.yaml
        yaml_path = metadata_dir / "derivatives" / "instrument_config.yaml"
        if yaml_path.is_file():
            try:
                with open(yaml_path, "r") as fh:
                    content = yaml.safe_load(fh)
                if isinstance(content, dict):
                    text = json.dumps(content)
                else:
                    text = str(content)
                if _contains_exaspim_keyword(text):
                    return True
            except Exception:
                logger.debug(
                    "Could not parse %s for exaSPIM detection",
                    yaml_path,
                )

        return False

    def run_job(self, job_settings: MapperJobSettings) -> None:
        """Upgrade exaSPIM metadata files in place.

        Uses ``job_settings.output_directory`` as the metadata
        directory.  ``input_filepath`` is not used directly since the
        mapper discovers files by convention.

        Parameters
        ----------
        job_settings : MapperJobSettings
            Job settings. ``output_directory`` is the metadata dir.

        Raises
        ------
        FileNotFoundError
            If ``acquisition.json`` is missing.
        RuntimeError
            If the upgrader fails to produce output.
        """
        metadata_dir = job_settings.output_directory
        acq_path = metadata_dir / "acquisition.json"
        acq_data = _load_json(acq_path)

        if acq_data is None:
            raise FileNotFoundError(
                f"acquisition.json not found at {acq_path}. "
                "This file is required for metadata upgrade."
            )

        if not _needs_upgrade(acq_data):
            logger.info(
                "acquisition.json is already schema_version %s "
                "(>= %s) — skipping upgrade.",
                acq_data.get("schema_version"),
                V2_THRESHOLD,
            )
            return

        logger.info(
            "acquisition.json is schema_version %s — upgrading …",
            acq_data.get("schema_version"),
        )

        inst_path = metadata_dir / "instrument.json"
        inst_data = _load_json(inst_path)

        if inst_data is None:
            inst_data = _resolve_instrument_from_yaml(metadata_dir)
            if inst_data is not None:
                logger.info(
                    "Resolved instrument from "
                    "instrument_config.yaml → writing %s",
                    inst_path,
                )
                _write_json(inst_path, inst_data)

        if inst_data is None:
            logger.warning(
                "No instrument.json at %s and could not resolve "
                "from instrument_config.yaml — upgrading "
                "acquisition with empty fluorescence_filters / "
                "light_sources.",
                inst_path,
            )

        # Upgrade
        if inst_data is not None:
            upgraded_acq, upgraded_inst = upgrade_with_instrument(
                acq_data, inst_data
            )
        else:
            upgraded_acq = upgrade_acquisition_only(acq_data)
            upgraded_inst = None

        if upgraded_acq is None:
            raise RuntimeError(
                "Upgrader did not produce an upgraded "
                "acquisition.json.  Check the input data and "
                "upgrader logs."
            )

        logger.info(
            "acquisition.json upgraded: %s → %s",
            acq_data.get("schema_version"),
            upgraded_acq.get("schema_version"),
        )

        # Write upgraded files back in place
        _write_json(acq_path, upgraded_acq)

        if upgraded_inst is not None:
            logger.info(
                "instrument.json upgraded: %s → %s",
                inst_data.get("schema_version"),
                upgraded_inst.get("schema_version"),
            )
            _write_json(inst_path, upgraded_inst)

        logger.info("exaSPIM metadata upgrade complete.")
