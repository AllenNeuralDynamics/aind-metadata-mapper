from datetime import datetime
import zoneinfo
from aind_metadata_mapper.fiber_photometry.session import FIBEtl
from aind_metadata_mapper.fiber_photometry.models import JobSettings

# need to get path to data from non-optional input args
# need to get start time from data
# need to get end time from data

# need to get subject_id and experimenter_full_name from non-optional input args
# need to get save path for output file from non-optional input args

# need to get data streams and all other unspecified fields from job_settings.json (need to create this file)

# Create example settings
session_start_time = GET FROM DATA
session_end_time = GET FROM DATA

settings = JobSettings(
    experimenter_full_name=["Test User"],
    session_start_time=session_start_time,
    subject_id="000000",
    rig_id="fiber_rig_01",
    mouse_platform_name="Disc",
    active_mouse_platform=False,
    data_streams=[{
        "stream_start_time": session_time,
        "stream_end_time": session_time,
        "light_sources": [{
            "name": "470nm LED",
            "excitation_power": 0.020,
            "excitation_power_unit": "milliwatt",
        }],
        "detectors": [{
            "name": "Hamamatsu Camera",
            "exposure_time": 10,
            "trigger_type": "Internal",
        }],
        "fiber_connections": [{
            "patch_cord_name": "Patch Cord A",
            "patch_cord_output_power": 40,
            "output_power_unit": "microwatt",
            "fiber_name": "Fiber A",
        }]
    }],
    notes="Test session",
    iacuc_protocol="2115",
)

# Generate session metadata
etl = FIBEtl(settings)
response = etl.run_job()