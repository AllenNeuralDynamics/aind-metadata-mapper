"""
File containing CamstimEphysSessionEtl class
"""

import datetime
import json
import logging
import re
from datetime import timedelta
from pathlib import Path
from typing import Union

import npc_mvr
import numpy as np
import pandas as pd
from aind_data_schema.components.coordinates import Coordinates3d
from aind_data_schema.components.devices import Software
from aind_data_schema.core.session import (
    ManipulatorModule,
    Session,
    StimulusEpoch,
    StimulusModality,
    Stream,
    VisualStimulation,
)
from aind_data_schema_models.modalities import Modality
from npc_ephys import (
    get_ephys_timing_on_sync,
    get_newscale_coordinates,
    get_single_oebin_path,
)

import aind_metadata_mapper.open_ephys.utils.pkl_utils as pkl
import aind_metadata_mapper.open_ephys.utils.sync_utils as sync
import aind_metadata_mapper.stimulus.camstim
from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.open_ephys.models import JobSettings

logger = logging.getLogger(__name__)


class CamstimEphysSessionEtl(
    aind_metadata_mapper.stimulus.camstim.Camstim, GenericEtl
):
    """
    An Ephys session, designed for OpenScope, employing neuropixel
    probes with visual and optogenetic stimulus from Camstim.
    """

    json_settings: dict = None
    session_path: Path
    recording_dir: Path

    def __init__(self, job_settings: Union[JobSettings, str, dict]) -> None:
        """
        Determine needed input filepaths from np-exp and lims, get session
        start and end times from sync file, write stim tables and extract
        epochs from stim tables. Also get available probes. If
        'overwrite_tables' is not given as True in the json settings, and
        existing stim table exists, a new one won't be written.
        'opto_conditions_map' may be given in the json settings to specify the
        different laser states for this experiment. Otherwise, the default is
        used from naming_utils.
        """
        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        elif isinstance(job_settings, dict):
            job_settings_model = JobSettings(**job_settings)
        else:
            job_settings_model = job_settings
        GenericEtl.__init__(self, job_settings=job_settings_model)

        # sessions_root = Path(self.job_settings.sessions_root)
        # self.folder_name = self.get_folder(session_id, sessions_root)
        # self.session_path = self.get_session_path(session_id, sessions_root)
        self.session_path = job_settings.input_source
        self.folder_name = self.session_path.name
        self.output_dir = job_settings.output_directory
        # sometimes data files are deleted on npexp so try files on lims
        # try:
        #     self.recording_dir = get_single_oebin_path(
        #         session_inst.lims_path
        #     ).parent
        # except:
        self.recording_dir = get_single_oebin_path(self.session_path).parent

        self.motor_locs_path = (
            self.session_path / f"{self.folder_name}.motor-locs.csv"
        )
        self.pkl_path = self.session_path / f"{self.folder_name}.stim.pkl"
        if not self.pkl_path.exists():
            self.pkl_path = (
                self.session_path / f"{self.folder_name}.behavior.pkl"
            )
        logger.debug("Using pickle:", self.pkl_path)
        self.pkl_data = pkl.load_pkl(self.pkl_path)
        self.fps = pkl.get_fps(self.pkl_data)

        self.opto_pkl_path = self.session_path / f"{self.folder_name}.opto.pkl"
        self.opto_table_path = (
            self.session_path / f"{self.folder_name}_opto_epochs.csv"
        )
        self.opto_conditions_map = job_settings.opto_conditions_map
        self.stim_table_path = (
            self.session_path / f"{self.folder_name}_stim_epochs.csv"
        )
        self.sync_path = self.session_path / f"{self.folder_name}.sync"

        platform_path = next(
            self.session_path.glob(f"{self.folder_name}_platform*.json")
        )
        self.platform_json = json.loads(platform_path.read_text())
        self.project_name = self.platform_json["project"]

        self.sync_data = sync.load_sync(self.sync_path)
        self.session_start = sync.get_start_time(self.sync_data)
        self.session_end = sync.get_stop_time(self.sync_data)
        logger.debug(
            f"session start: {self.session_start} \n"
            f" session end: {self.session_end}"
        )

        self.session_uuid = self.get_session_uuid()
        self.mtrain_server = job_settings.mtrain_server
        self.stage_name = pkl.get_stage(self.pkl_data)
        self.behavior = self._is_behavior()

        if not self.stim_table_path.exists() or (
            self.job_settings.overwrite_tables
        ):
            logger.debug("building stim table")
            if self.behavior:
                self.build_behavior_table()
            else:
                self.build_stimulus_table()
        if self.opto_pkl_path.exists() and (
            not self.opto_table_path.exists()
            or self.job_settings.overwrite_tables
        ):
            logger.debug("building opto table")
            self.build_optogenetics_table()

        logger.debug("getting stim epochs")
        self.stim_epochs = self.epochs_from_stim_table()
        if self.opto_table_path.exists():
            self.stim_epochs.append(self.epoch_from_opto_table())

        self.available_probes = self.get_available_probes()

    def run_job(self):
        """Transforms all metadata for the session into relevant files"""
        self._extract()
        self._transform()
        return self._load(self.session_json, self.output_dir)

    def _extract(self):
        """TODO: refactor a lot of the __init__ code here"""
        pass

    def _transform(self) -> Session:
        """
        Creates the session schema json
        """
        self.session_json = Session(
            experimenter_full_name=[
                self.platform_json["operatorID"].replace(".", " ").title()
            ],
            session_start_time=self.session_start,
            session_end_time=self.session_end,
            session_type=self.job_settings.session_type,
            iacuc_protocol=self.job_settings.iacuc_protocol,
            rig_id=self.platform_json["rig_id"],
            subject_id=self.folder_name.split("_")[1],
            data_streams=self.data_streams(),
            stimulus_epochs=self.stim_epochs,
            mouse_platform_name=self.job_settings.mouse_platform_name,
            active_mouse_platform=self.job_settings.active_mouse_platform,
            reward_consumed_unit="milliliter",
            notes="",
        )
        return self.session_json

    def get_folder(self, session_id, npexp_root) -> str:
        """returns the directory name of the session on the np-exp directory"""
        for subfolder in npexp_root.iterdir():
            if subfolder.name.split("_")[0] == session_id:
                return subfolder.name
        else:
            raise Exception("Session folder not found in np-exp")

    def get_session_path(self, session_id, npexp_root) -> Path:
        """returns the path to the session on allen's np-exp directory"""
        return npexp_root / self.get_folder(session_id, npexp_root)

    @staticmethod
    def extract_probe_letter(probe_exp, s):
        """
        Extracts probe letter from a string.
        """
        match = re.search(probe_exp, s)
        if match:
            return match.group("letter")

    def get_available_probes(self) -> tuple[str]:
        """
        Returns a list of probe letters among ABCDEF that are inserted
        according to platform.json. If platform.json has no insertion record,
        returns all probes (this could cause problems).
        """
        insertion_notes = self.platform_json["InsertionNotes"]
        if insertion_notes == {}:
            available_probes = "ABCDEF"
        else:
            available_probes = [
                letter
                for letter in "ABCDEF"
                if not insertion_notes.get(f"Probe{letter}", {}).get(
                    "FailedToInsert", False
                )
            ]
        logger.debug("available probes:", available_probes)
        return tuple(available_probes)

    @staticmethod
    def manipulator_coords(
        probe_name: str, newscale_coords: pd.DataFrame
    ) -> tuple[Coordinates3d, str]:
        """
        Returns the schema coordinates object containing probe's manipulator
        coordinates accrdong to newscale, and associated 'notes'. If the
        newscale coords don't include this probe (shouldn't happen), return
        coords with 0.0s and notes indicating no coordinate info available
        """
        try:
            probe_row = newscale_coords.query(
                f"electrode_group == '{probe_name}'"
            )
        except pd.errors.UndefinedVariableError:
            probe_row = newscale_coords.query(
                f"electrode_group_name == '{probe_name}'"
            )
        if probe_row.empty:
            return (
                Coordinates3d(x="0.0", y="0.0", z="0.0", unit="micrometer"),
                "Coordinate info not available",
            )
        else:
            x, y, z = (
                probe_row["x"].item(),
                probe_row["y"].item(),
                probe_row["z"].item(),
            )
        return (
            Coordinates3d(x=x, y=y, z=z, unit="micrometer"),
            "",
        )

    def ephys_modules(self) -> list:
        """
        Return list of schema ephys modules for each available probe.
        """
        newscale_coords = get_newscale_coordinates(self.motor_locs_path)

        ephys_modules = []
        for probe_letter in self.available_probes:
            probe_name = f"probe{probe_letter}"
            manipulator_coordinates, notes = self.manipulator_coords(
                probe_name, newscale_coords
            )

            probe_module = ManipulatorModule(
                assembly_name=probe_name.upper(),
                arc_angle=0.0,
                module_angle=0.0,
                rotation_angle=0.0,
                primary_targeted_structure="root",
                manipulator_coordinates=manipulator_coordinates,
                notes=notes,
            )
            ephys_modules.append(probe_module)
        return ephys_modules

    def ephys_stream(self) -> Stream:
        """
        Returns schema ephys datastream, including the list of ephys modules
        and the ephys start and end times.
        """
        probe_exp = r"(?<=[pP{1}]robe)[-_\s]*(?P<letter>[A-F]{1})(?![a-zA-Z])"

        times = get_ephys_timing_on_sync(
            sync=self.sync_path, recording_dirs=[self.recording_dir]
        )

        ephys_timing_data = tuple(
            timing
            for timing in times
            if (p := self.extract_probe_letter(probe_exp, timing.device.name))
            is None
            or p in self.available_probes
        )

        stream_first_time = min(
            timing.start_time for timing in ephys_timing_data
        )
        stream_last_time = max(
            timing.stop_time for timing in ephys_timing_data
        )

        return Stream(
            stream_start_time=self.session_start
            + datetime.timedelta(seconds=stream_first_time),
            stream_end_time=self.session_start
            + datetime.timedelta(seconds=stream_last_time),
            ephys_modules=self.ephys_modules(),
            stick_microscopes=[],
            stream_modalities=[Modality.ECEPHYS],
        )

    def sync_stream(self) -> Stream:
        """
        Returns schema behavior stream for the sync timing.
        """
        return Stream(
            stream_start_time=self.session_start,
            stream_end_time=self.session_end,
            stream_modalities=[Modality.BEHAVIOR],
            daq_names=["Sync"],
        )

    def video_stream(self) -> Stream:
        """
        Returns schema behavior videos stream for video timing
        """
        video_frame_times = npc_mvr.mvr.get_video_frame_times(
            self.sync_path, self.session_path
        )

        stream_first_time = min(
            np.nanmin(timestamps) for timestamps in video_frame_times.values()
        )
        stream_last_time = max(
            np.nanmax(timestamps) for timestamps in video_frame_times.values()
        )

        return Stream(
            stream_start_time=self.session_start
            + datetime.timedelta(seconds=stream_first_time),
            stream_end_time=self.session_start
            + datetime.timedelta(seconds=stream_last_time),
            camera_names=["Front camera", "Side camera", "Eye camera"],
            stream_modalities=[Modality.BEHAVIOR_VIDEOS],
        )

    def data_streams(self) -> tuple[Stream, ...]:
        """
        Return three schema datastreams; ephys, behavior, and behavior videos.
        May be extended.
        """
        data_streams = []
        data_streams.append(self.ephys_stream())
        # data_streams.append(self.sync_stream())
        # data_streams.append(self.video_stream())
        return tuple(data_streams)

    def build_optogenetics_table(self):
        """
        Builds an optogenetics table from the opto pickle file and sync file.
        Writes the table to a csv file.
        Parameters
        ----------
        output_opto_table_path : str
            Path to write the optogenetics table to.
        returns
        -------
        dict
            Dictionary containing the path to the output opto table
        """
        opto_file = pkl.load_pkl(self.opto_pkl_path)
        sync_file = sync.load_sync(self.sync_path)
        start_times = sync.extract_led_times(
            sync_file, self.opto_conditions_map
        )
        condition_nums = [str(item) for item in opto_file["opto_conditions"]]
        levels = opto_file["opto_levels"]
        assert len(condition_nums) == len(levels)
        if len(start_times) > len(condition_nums):
            raise ValueError(
                f"there are {len(start_times) - len(condition_nums)} extra "
                f"optotagging sync times!"
            )
        optotagging_table = pd.DataFrame(
            {
                "start_time": start_times,
                "condition_num": condition_nums,
                "level": levels,
            }
        )
        optotagging_table = optotagging_table.sort_values(
            by="start_time", axis=0
        )
        stop_times = []
        conditions = []
        names = []
        for _, row in optotagging_table.iterrows():
            condition = self.opto_conditions_map[row["condition_num"]]
            stop_times.append(row["start_time"] + condition["duration"])
            conditions.append(condition["condition"])
            names.append(condition["name"])
        optotagging_table["stop_time"] = stop_times
        optotagging_table["condition"] = conditions
        optotagging_table["name"] = names
        optotagging_table["duration"] = (
            optotagging_table["stop_time"] - optotagging_table["start_time"]
        )
        optotagging_table.to_csv(self.opto_table_path, index=False)

    def epoch_from_opto_table(self) -> StimulusEpoch:
        """
        From the optogenetic stimulation table, returns a single schema
        stimulus epoch representing the optotagging period. Include all
        unknown table columns (not start_time, stop_time, stim_name) as
        parameters, and include the set of all of that column's values as the
        parameter values.
        """
        script_obj = Software(
            name=self.stage_name,
            version="1.0",
        )

        opto_table = pd.read_csv(self.opto_table_path)

        opto_params = {}
        for column in opto_table:
            if column in ("start_time", "stop_time", "stim_name"):
                continue
            param_set = set(opto_table[column].dropna())
            opto_params[column] = param_set

        params_obj = VisualStimulation(
            stimulus_name="Optogenetic Stimulation",
            stimulus_parameters=opto_params,
            stimulus_template_name=[],
        )

        opto_epoch = StimulusEpoch(
            stimulus_start_time=self.session_start
            + timedelta(seconds=opto_table.start_time.iloc[0]),
            stimulus_end_time=self.session_start
            + timedelta(seconds=opto_table.start_time.iloc[-1]),
            stimulus_name="Optogenetic Stimulation",
            software=[],
            script=script_obj,
            stimulus_modalities=[StimulusModality.OPTOGENETICS],
            stimulus_parameters=[params_obj],
        )

        return opto_epoch


def main() -> None:
    """
    Run Main
    """
    sessionETL = CamstimEphysSessionEtl(**vars)
    sessionETL.run_job()


if __name__ == "__main__":
    main()
