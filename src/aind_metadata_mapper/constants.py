"""Shared constants.

The aind-metadata-service host and the endpoint paths used across the package, plus the list of in
vivo modalities that gates the acquisition populate step. Each path is defined once here so that
``JobSettings``, the fetch helpers in ``utils`` and the steps in ``populate`` agree on it. Paths
carry a leading slash and no trailing slash. Callers join them onto a host and add the trailing slash
themselves where needed.
"""

from aind_data_schema_models.modalities import Modality

METADATA_SERVICE_URL = "http://aind-metadata-service"

# Endpoint paths, relative to the metadata service host.
SUBJECT_ENDPOINT = "/api/v2/subject"
PROCEDURES_ENDPOINT = "/api/v2/procedures"
INSTRUMENT_ENDPOINT = "/api/v2/instrument"
LABTRACKS_SUBJECT_ENDPOINT = "/api/v2/labtracks/subject"
MOUSE_WEIGHT_RECORDS_ENDPOINT = "/api/v2/dataverse/mouse_weight_records"
INTENDED_MEASUREMENTS_ENDPOINT = "/intended_measurements"
DATAVERSE_TABLES_ENDPOINT = "/api/v2/dataverse/tables"

# Dataverse mice table, used to look up a subject's project when project_name is not provided.
# These names are set by the Dataverse environment. They will change when the production environment
# is migrated, so they are defined once here.
DATAVERSE_MICE_TABLE = "aibs_dim_mices"
DATAVERSE_MOUSE_ID_COLUMN = "aibs_mouse_id"
DATAVERSE_PROJECT_COLUMN = "_aibs_project_name_value"
# The project is a lookup column (a link to the projects table). Dataverse returns a lookup as the
# linked row's id plus its display name under an annotated key, and the metadata service keeps only
# the display name, so this is the key the project name comes back under.
DATAVERSE_PROJECT_NAME_KEY = DATAVERSE_PROJECT_COLUMN + "@OData.Community.Display.V1.FormattedValue"

# The same endpoints on the default host, for helpers called outside a ``GatherMetadataJob``.
SUBJECT_BASE_URL = METADATA_SERVICE_URL + SUBJECT_ENDPOINT
PROCEDURES_BASE_URL = METADATA_SERVICE_URL + PROCEDURES_ENDPOINT
INSTRUMENT_BASE_URL = METADATA_SERVICE_URL + INSTRUMENT_ENDPOINT
LABTRACKS_SUBJECT_BASE_URL = METADATA_SERVICE_URL + LABTRACKS_SUBJECT_ENDPOINT
INTENDED_MEASUREMENTS_BASE_URL = METADATA_SERVICE_URL + INTENDED_MEASUREMENTS_ENDPOINT
DATAVERSE_TABLES_BASE_URL = METADATA_SERVICE_URL + DATAVERSE_TABLES_ENDPOINT

# Modalities acquired from a living animal. The populate step (weights, IACUC protocol) runs only
# when data_description.modalities includes at least one of these.
IN_VIVO_MODALITIES: frozenset[str] = frozenset(
    modality.abbreviation
    for modality in (
        Modality.BEHAVIOR,
        Modality.BEHAVIOR_VIDEOS,
        Modality.ECEPHYS,
        Modality.EMG,
        Modality.FIB,
        Modality.ISI,
        Modality.MRI,
        Modality.ONE_PHOTON,
        Modality.POPHYS,
        Modality.SLAP2,
    )
)
