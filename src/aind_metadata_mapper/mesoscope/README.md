# Mesoscope mapper

This mapper transforms extractor-produced `mesoscope.json` into
`acquisition_mesoscope.json` for `GatherMetadataJob`.

## Input contract

The mapper consumes the `aind-metadata-extractor` `MesoscopeExtractModel`,
including `tiff_header`, `session_metadata`, `camstim_epchs`,
`camstim_session_type`, and `job_settings`.

`job_settings.instrument_id` is required. Preparation should populate this
field from the raw `rig.json` `rig_id` when that full instrument identifier is
available. The session or platform alias such as `MESO.2` must not be promoted
to `Acquisition.instrument_id`.

## Current mapping

The mapper emits one `ImagingConfig` for the mesoscope stream. It uses the
explicit `job_settings.instrument_id` value for `Acquisition.instrument_id` and
preserves the platform or session alias such as `MESO.2` for
`ImagingConfig.device_name`, matching current production upgrade outputs.

Plane power preserves the legacy percent values. Calculated milliwatt values
are not mapped.

Naive timestamps are interpreted in `America/Los_Angeles`, while aware timestamps
are preserved. Acquisition bounds span the recorded stream and stimulus epochs
without altering their individual timestamps.

The mapper writes the acquisition coordinate system through the
`coordinate_system` field so that it remains compatible with the declared
`aind-data-schema>=2.7.1,<3` dependency floor. Version 2.7.1 does not expose
`global_coordinate_system`.

Behavior, Eye, and Face camera names are normalized to upgrader-style assembly
names such as `Behavior camera assembly`.

Missing `stimulus_modalities` are normalized to `Visual`, and omitted
visual-stimulation parameter defaults (`stimulus_type` and `notes`) are filled
from the historical mesoscope Camstim producer contract. This keeps native
mapper output aligned with legacy and upgraded production metadata without
overwriting any values that are already supplied by extraction.

## Inherited limits

The nested `Unknown Detector`, `Laser`, and `Ophys Channel` values are
deliberate scientific compatibility defaults inherited from the legacy mapper
and upgrader outputs. Detector and channel-index inference are not included.

## Sources

The implementation follows these sources:

- Legacy mapper:
  `v0.29.3:src/aind_metadata_mapper/mesoscope/session.py`
- Legacy Camstim producer contract:
  `v0.29.3:src/aind_metadata_mapper/stimulus/camstim.py`
- Released upgrader precedent:
  `v0.17.12:src/aind_metadata_upgrader/session/v1v2.py`
- Extractor contract:
  `aind_metadata_extractor/models/mesoscope.py`
