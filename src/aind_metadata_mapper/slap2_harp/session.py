import logging
from pathlib import Path
import shutil
from typing import Union
import requests
import yaml
import harp
import io
import os
from datetime import datetime, timedelta

from aind_data_schema.core.session import Session
from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.slap2_harp.models import JobSettings
from aind_data_schema.core.session import Stream
from aind_data_schema_models.modalities import Modality

logger = logging.getLogger(__name__)

# HARP utility functions for timing alignment
def _get_who_am_i_list(url: str = "https://raw.githubusercontent.com/harp-tech/protocol/main/whoami.yml"):
    response = requests.get(url, allow_redirects=True, timeout=5)
    content = response.content.decode("utf-8")
    content = yaml.safe_load(content)
    devices = content["devices"]
    return devices

def _get_yml_from_who_am_i(who_am_i: int, release: str = "main") -> io.BytesIO:
    try:
        device = _get_who_am_i_list()[who_am_i]
    except KeyError as e:
        raise KeyError(f"WhoAmI {who_am_i} not found in whoami.yml") from e

    repository_url = device.get("repositoryUrl", None)

    if (repository_url is None):
        raise ValueError("Device's repositoryUrl not found in whoami.yml")
    else:  # attempt to get the device.yml from the repository
        _repo_hint_paths = [
            "{repository_url}/{release}/device.yml",
            "{repository_url}/{release}/software/bonsai/device.yml",
        ]

        yml = None
        for hint in _repo_hint_paths:
            url = hint.format(repository_url=repository_url, release=release)
            if "github.com" in url:
                url = url.replace("github.com", "raw.githubusercontent.com")
            response = requests.get(url, allow_redirects=True, timeout=5)
            if response.status_code == 200:
                yml = io.BytesIO(response.content)
                break
        if yml is None:
            raise FileNotFoundError("device.yml not found in any repository")
        else:
            return yml

def fetch_yml(harp_path):
    with open(harp_path / 'Behavior_0.bin', mode='rb') as reg_0:
        who_am_i = int(harp.read(reg_0).values[0][0])
        yml_bytes = _get_yml_from_who_am_i(who_am_i)
    yaml_content = yml_bytes.getvalue()
    with open(harp_path / "device.yml", "wb") as f:
        f.write(yaml_content)
    return harp_path / "device.yml"


class Slap2HarpSessionEtl(GenericEtl):
    """
    A generic ETL for SLAP2 Harp sessions.
    """

    session_path: Path
    output_dir: Path

    def __init__(self, job_settings: Union[JobSettings, str, dict]) -> None:
        """
        Initialize ETL with job settings.
        """
        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        elif isinstance(job_settings, dict):
            job_settings_model = JobSettings(**job_settings)
        else:
            job_settings_model = job_settings
        GenericEtl.__init__(self, job_settings=job_settings_model)

        self.session_path = job_settings.input_source
        self.output_dir = job_settings.output_directory
        logger.debug(f"Initialized SLAP2 Harp ETL for {self.session_path}")

    def get_timing(self, harp_path):
        """
        Extract session start and end times from HARP files and set as class attributes.

        Parameters
        ----------
        harp_path : str or Path
            Path to the HARP data directory
        """
        harp_path = Path(harp_path)
        reader = harp.create_reader(harp_path)

        # Get SLAP2 trial start and end times
        start_trial_times = reader.PulseDO0.read()["PulseDO0"].index.to_numpy()[2:]
        end_trial_times = reader.PulseDO1.read().index.to_numpy()[2:]
        self.harp_time_offset = start_trial_times[0]
        norm_end_trial_times = end_trial_times - self.harp_time_offset

        timestamp_str = self.session_path.name.split('_')[-1]
        session_start_datetime = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S")

        self.session_start = session_start_datetime
        self.session_end = session_start_datetime + timedelta(seconds=norm_end_trial_times[-1])

    def extract_mouse_id(self):
        """
        Extract mouse id from the session_path folder name (first 6 digit number)
        and set as class attribute. For example, from '794237_20250508T145040',
        extract '794237'.
        """
        import re
        match = re.search(r"(\d{6})", self.session_path.name)
        if match:
            self.mouse_id = match.group(1)
        else:
            self.mouse_id = None
        logger.debug(f"Extracted mouse_id: {self.mouse_id}")
        print(f"Extracted mouse_id: {self.mouse_id}")

    def run_job(self):
        """Transforms all metadata for the session into relevant files"""
        self._extract()
        self._transform()
        return self._load(self.session_json, self.output_dir)

    def _extract(self):
        """
        Extract raw data and metadata from session files.
        """
        # Find the harp folder as a subdirectory ending with .harp
        harp_dirs = list(self.session_path.rglob("*.harp"))
        if not harp_dirs:
            raise FileNotFoundError("No .harp directory found in session path")
        if len(harp_dirs) > 1:
            raise RuntimeError(f"Multiple .harp directories found in session path: {harp_dirs}")
        self.harp_path = harp_dirs[0]

        # Ensure device.yml exists
        if not (self.harp_path / "device.yml").exists():
            logger.info("device.yml not found, fetching from the web")
            # fetch_yml should be defined elsewhere or imported
            fetch_yml(self.harp_path)

        # Instantiate and assign the harp reader as an attribute
        self.harp_reader = harp.create_reader(self.harp_path)

        # Continue with timing and mouse id extraction
        self.get_timing(self.harp_path)
        self.extract_mouse_id()
        logger.debug("Extracting data for SLAP2 Harp session.")

    def make_harp_data_stream(self, channel_name, stream_type="analog"):
        """
        Create a Stream object for a given HARP channel.
        Parameters
        ----------
        channel_name : str
            Name of the channel (e.g., 'AnalogInput0', 'Encoder', 'PulseDO0', etc.)
        stream_type : str
            'analog' or 'digital'
        Returns
        -------
        Stream (aind_data_schema.core.session.Stream)
        """
        # Read the data
        if stream_type == "analog":
            data = self.harp_reader.AnalogData.read()
            times = data.index.to_numpy()
        elif stream_type == "digital":
            data = getattr(self.harp_reader, channel_name).read()
            times = data[channel_name].index.to_numpy()
        else:
            raise ValueError(f"Unknown stream_type: {stream_type}")
        normalized_times = [float(t)-self.harp_time_offset for t in times]

        # Assign modality: SLAP for DO0/DO1, otherwise BEHAVIOR
        if channel_name in ["PulseDO0", "PulseDO1"]:
            modality = Modality.SLAP
        else:
            modality = Modality.BEHAVIOR
        # Use the first and last time as stream start/end (relative to session start)
        stream_start_time = self.session_start + timedelta(seconds=normalized_times[0])
        stream_end_time = self.session_start + timedelta(seconds=normalized_times[-1])
        return Stream(
            stream_start_time=stream_start_time,
            stream_end_time=stream_end_time,
            stream_modalities=[modality],
            daq_names=[channel_name],
            notes=f"HARP {stream_type} channel"
        )

    def make_data_streams(self):
        """
        Create data streams for the session from HARP analog and digital channels.
        Returns a list of Stream objects.
        """
        streams = []
        # Analog channels
        for analog_channel in ["AnalogInput0", "Encoder"]:
            streams.append(self.make_harp_data_stream(analog_channel, stream_type="analog"))
        # Digital channels
        for digital_channel in ["PulseDO0", "PulseDO1", "PulseDO2"]:
            streams.append(self.make_harp_data_stream(digital_channel, stream_type="digital"))
        return streams

    def _transform(self) -> Session:
        """
        Transform extracted data into Session schema.
        """
        self.session_json = Session(
            experimenter_full_name=[],
            session_start_time=self.session_start,
            session_end_time=self.session_end,
            session_type=self.job_settings.session_type,
            iacuc_protocol=self.job_settings.iacuc_protocol,
            rig_id="SLAP2_1",
            subject_id=self.mouse_id,
            data_streams=self.make_data_streams(),
            stimulus_epochs=[],
            mouse_platform_name=self.job_settings.mouse_platform_name,
            active_mouse_platform=self.job_settings.active_mouse_platform,
            reward_consumed_unit="milliliter",
            notes="",
        )
        logger.debug("Transformed data into Session schema.")

        return self.session_json

    # Add additional methods as needed for SLAP2 Harp specifics

def main() -> None:
    """
    Run Main
    """
    # Replace 'vars' with actual job settings or argument parsing
    sessionETL = Slap2HarpSessionEtl(**vars)
    sessionETL.run_job()

if __name__ == "__main__":
    main()
