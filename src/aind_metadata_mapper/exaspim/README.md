# exaSPIM Metadata Mapper

Pre-processor that detects exaSPIM metadata and upgrades v1 `acquisition.json`
/ `instrument.json` to aind-data-schema v2.

## Detection

The mapper checks, in order:

1. `instrument.json` — does `instrument_id` or `instrument_type` contain
   "exaspim" (case-insensitive)?
2. Fall-back: does a `derivatives/instrument_config.yaml` file in the metadata
   directory contain "exaspim" (case-insensitive)?

## What it does

* **Already v2 (schema_version ≥ 2.0.0):** no-op.
* **v1 with instrument.json:** sanitises manufacturer names and deprecated
  fields, then upgrades both files via `aind-metadata-upgrader`.
* **v1 without instrument.json:** upgrades acquisition only, using an empty
  filter / light-source stub.

Upgraded files are written back **in place** in the metadata directory.
S3 upload is the caller's responsibility (e.g. `aind-data-transfer-service`).

## Integration

Registered in the `pre_processor_registry` of `mapper_registry.py`.
`GatherMetadataJob` runs all matching pre-processors before the normal
metadata-gather flow.
