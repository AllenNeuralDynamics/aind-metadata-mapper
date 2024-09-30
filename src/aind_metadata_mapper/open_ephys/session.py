"""Module to write valid open_ephys schemas"""

from dataclasses import dataclass
from datetime import datetime
from typing import Union, List

from aind_data_schema.core.session import Session
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.open_ephys.models import JobSettings


@dataclass(frozen=True)
class ParsedInformation:
    """RawImageInfo gets parsed into this data"""

    stage_logs: List[str]
    openephys_logs: List[str]
    experiment_data: dict


class EphysEtl(GenericEtl[JobSettings]):
    """This class contains the methods to write open_ephys session"""

    def __init__(self, job_settings: Union[JobSettings, str]):
        """
        Class constructor
        Parameters
        ----------
        job_settings: Union[JobSettings, str]
          Variables for a particular session
        """

        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        else:
            job_settings_model = job_settings

        super().__init__(job_settings=job_settings_model)

    def _transform(self, extracted_source: ParsedInformation) -> Session:
        """
        Parses params from stage_log and openephys_log and
        creates partial open_ephys session model
        Parameters
        ----------
        extracted_source : ParsedInformation

        Returns
        -------
        Session

        """

        stage_logs = extracted_source.stage_logs
        openephys_logs = extracted_source.openephys_logs
        experiment_data = extracted_source.experiment_data

        ephys_session = {}

        # Process data from dictionary keys
        start_time = (
            openephys_logs[0]
            .getElementsByTagName("DATE")[0]
            .firstChild.nodeValue
        )
        ephys_session["session_start_time"] = datetime.strptime(
            start_time, "%d %b %Y %H:%M:%S"
        )
        ephys_session["experimenter_full_name"] = experiment_data[
            "experimenter_full_name"
        ]
        ephys_session["subject_id"] = experiment_data["subject_id"]
        ephys_session["session_type"] = experiment_data["session_type"]
        ephys_session["iacuc_protocol"] = experiment_data["iacuc_protocol"]
        ephys_session["rig_id"] = experiment_data["rig_id"]
        ephys_session["animal_weight_prior"] = experiment_data[
            "animal_weight_prior"
        ]
        ephys_session["maintenance"] = experiment_data["maintenance"]
        ephys_session["calibrations"] = experiment_data["calibrations"]

        # Constant throughout data streams
        stick_microscopes = experiment_data["stick_microscopes"]
        camera_names = experiment_data["camera_names"]
        daqs = experiment_data["daqs"]
        ephys_session["data_streams"] = []

        for stage, data_stream in zip(
            stage_logs, experiment_data["data_streams"]
        ):
            session_stream = {}
            session_stream["stream_start_time"] = datetime.strptime(
                stage[0][0], "%Y/%m/%d %H:%M:%S.%f"
            )
            session_stream["stream_end_time"] = datetime.strptime(
                stage[-1][0], "%Y/%m/%d %H:%M:%S.%f"
            )
            session_stream["stream_modalities"] = [Modality.ECEPHYS]
            session_stream["stick_microscopes"] = stick_microscopes
            session_stream["camera_names"] = camera_names
            session_stream["daq_names"] = [daqs]
            session_stream["ephys_modules"] = []
            stage_info = [
                x for i, x in enumerate(stage) if x[1] != stage[i - 1][1]
            ]  # isolate first log statement of probes
            for info in stage_info:
                probe = info[1][3:]  # remove SN
                ephys_module = data_stream[f"ephys_module_{probe}"]
                ephys_module["assembly_name"] = probe
                ephys_module["manipulator_coordinates"] = {
                    axis: info[i]
                    for axis, i in zip(["x", "y", "z"], [2, 3, 4])
                }
                ephys_module["ephys_probes"] = [{"name": probe}]

                session_stream["ephys_modules"].append(ephys_module)

            ephys_session["data_streams"].append(session_stream)

        ephys_session["mouse_platform_name"] = data_stream[
            "mouse_platform_name"
        ]
        ephys_session["active_mouse_platform"] = data_stream[
            "active_mouse_platform"
        ]

        end_times = [
            datetime.strptime(x[-1][0], "%Y/%m/%d %H:%M:%S.%f")
            for x in stage_logs
        ]
        ephys_session["session_end_time"] = max(end_times)
        return Session(**ephys_session)

    def _extract(self) -> ParsedInformation:
        """Extract metadata from open_ephys session."""
        return ParsedInformation(
            stage_logs=self.job_settings.stage_logs,
            openephys_logs=self.job_settings.openephys_logs,
            experiment_data=self.job_settings.experiment_data,
        )
