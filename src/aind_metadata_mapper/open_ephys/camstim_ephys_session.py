"""
File containing CamstimEphysSession class
"""

import argparse
import datetime
import json
import logging
import re
from pathlib import Path

import np_session
import npc_ephys
import npc_mvr
import npc_sessions
import numpy as np
import pandas as pd
from aind_data_schema.components.coordinates import Coordinates3d
from aind_data_schema.core.session import (
    ManipulatorModule,
    Session,
    Stream,
)
from aind_data_schema_models.modalities import Modality

import aind_metadata_mapper.open_ephys.utils.constants as constants
import aind_metadata_mapper.open_ephys.utils.sync_utils as sync
import aind_metadata_mapper.stimulus.camstim

logger = logging.getLogger(__name__)


class CamstimEphysSession(aind_metadata_mapper.stimulus.camstim.Camstim):
    """
    An Ephys session, designed for OpenScope, employing neuropixel
    probes with visual and optogenetic stimulus from Camstim.
    """

    json_settings: dict = None
    npexp_path: Path
    recording_dir: Path

    def __init__(self, session_id: str, json_settings: dict) -> None:
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
        if json_settings.get("opto_conditions_map", None) is None:
            self.opto_conditions_map = constants.DEFAULT_OPTO_CONDITIONS
        else:
            self.opto_conditions_map = json_settings["opto_conditions_map"]
        overwrite_tables = json_settings.get("overwrite_tables", False)

        self.json_settings = json_settings
        session_inst = np_session.Session(session_id)
        self.mtrain = session_inst.mtrain
        self.npexp_path = session_inst.npexp_path
        self.folder = session_inst.folder
        # sometimes data files are deleted on npexp so try files on lims
        try:
            self.recording_dir = npc_ephys.get_single_oebin_path(
                session_inst.lims_path
            ).parent
        except FileNotFoundError:
            self.recording_dir = npc_ephys.get_single_oebin_path(
                session_inst.npexp_path
            ).parent

        self.motor_locs_path = (
            self.npexp_path / f"{self.folder}.motor-locs.csv"
        )
        self.pkl_path = self.npexp_path / f"{self.folder}.stim.pkl"
        self.opto_pkl_path = self.npexp_path / f"{self.folder}.opto.pkl"
        self.opto_table_path = (
            self.npexp_path / f"{self.folder}_opto_epochs.csv"
        )
        self.stim_table_path = (
            self.npexp_path / f"{self.folder}_stim_epochs.csv"
        )
        self.sync_path = self.npexp_path / f"{self.folder}.sync"

        platform_path = next(
            self.npexp_path.glob(f"{self.folder}_platform*.json")
        )
        self.platform_json = json.loads(platform_path.read_text())
        self.project_name = self.platform_json["project"]

        sync_data = sync.load_sync(self.sync_path)
        self.session_start = sync.get_start_time(sync_data)
        self.session_end = sync.get_stop_time(sync_data)
        logger.debug(
            f"session start: {self.session_start} \n"
            f" session end: {self.session_end}"
        )

        if not self.stim_table_path.exists() or overwrite_tables:
            logger.debug("building stim table")
            self.build_stimulus_table()
        if (
            self.opto_pkl_path.exists()
            and not self.opto_table_path.exists()
            or overwrite_tables
        ):
            logger.debug("building opto table")
            self.build_optogenetics_table()

        logger.debug("getting stim epochs")
        self.stim_epochs = self.epochs_from_stim_table()
        if self.opto_table_path.exists():
            self.stim_epochs.append(self.epoch_from_opto_table())

        self.available_probes = self.get_available_probes()

    def generate_session_json(self) -> Session:
        """
        Creates the session schema json
        """
        self.session_json = Session(
            experimenter_full_name=[
                self.platform_json["operatorID"].replace(".", " ").title()
            ],
            session_start_time=self.session_start,
            session_end_time=self.session_end,
            session_type=self.json_settings.get("session_type", ""),
            iacuc_protocol=self.json_settings.get("iacuc_protocol", ""),
            rig_id=self.platform_json["rig_id"],
            subject_id=self.folder.split("_")[1],
            data_streams=self.data_streams(),
            stimulus_epochs=self.stim_epochs,
            mouse_platform_name=self.json_settings.get(
                "mouse_platform", "Mouse Platform"
            ),
            active_mouse_platform=self.json_settings.get(
                "active_mouse_platform", False
            ),
            reward_consumed_unit="milliliter",
            notes="",
        )
        return self.session_json

    def write_session_json(self) -> None:
        """
        Writes the session json to a session.json file
        """
        self.session_json.write_standard_file(self.npexp_path)
        logger.debug(f"File created at {str(self.npexp_path)}/session.json")

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
        newscale_coords = npc_sessions.get_newscale_coordinates(
            self.motor_locs_path
        )

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
                primary_targeted_structure="none",
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

        times = npc_ephys.get_ephys_timing_on_sync(
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
            self.sync_path, self.npexp_path
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
        data_streams.append(self.sync_stream())
        data_streams.append(self.video_stream())
        return tuple(data_streams)


def parse_args() -> argparse.Namespace:
    """
    Parse Arguments
    """
    parser = argparse.ArgumentParser(
        description="Generate a session.json file for an ephys session"
    )
    parser.add_argument(
        "session_id",
        help=(
            "session ID (lims or np-exp foldername) or path to session"
            "folder"
        ),
    )
    parser.add_argument(
        "json-settings",
        help=(
            'json containing at minimum the fields "session_type" and'
            '"iacuc protocol"'
        ),
    )
    return parser.parse_args()


def main() -> None:
    """
    Run Main
    """
    sessionETL = CamstimEphysSession(**vars(parse_args()))
    sessionETL.generate_session_json()


if __name__ == "__main__":
    main()
