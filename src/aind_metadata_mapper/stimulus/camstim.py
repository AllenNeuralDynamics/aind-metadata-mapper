"""
File containing Camstim class
"""

import functools
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from aind_data_schema.components.devices import Software
from aind_data_schema.core.session import (
    StimulusEpoch,
    StimulusModality,
    VisualStimulation,
)
from pydantic import BaseModel

import aind_metadata_mapper.open_ephys.utils.naming_utils as names
import aind_metadata_mapper.open_ephys.utils.pkl_utils as pkl
import aind_metadata_mapper.open_ephys.utils.sync_utils as sync
from aind_metadata_mapper.open_ephys.utils import (
    behavior_utils,
    constants,
    stim_utils,
)


class CamstimSettings(BaseModel):
    """Camstim settings for extracting stimulus epochs"""

    sessions_root: Optional[Path] = None
    opto_conditions_map: Optional[dict] = None
    overwrite_tables: bool = False
    input_source: Path
    output_directory: Optional[Path]
    session_id: str
    subject_id: str


class Camstim:
    """
    Methods used to extract stimulus epochs
    """

    def __init__(
        self,
        camstim_settings: CamstimSettings,
    ) -> None:
        """
        Determine needed input filepaths from np-exp and lims, get session
        start and end times from sync file, write stim tables and extract
        epochs from stim tables. If 'overwrite_tables' is not given as True,
        in the json settings and an existing stim table exists, a new one
        won't be written. opto_conditions_map may be given in the json
        settings to specify the different laser states for this experiment.
        Otherwise, the default is used from naming_utils.
        """
        self.sync_path = None
        self.sync_data = None
        self.camstim_settings = camstim_settings
        self.input_source = Path(self.camstim_settings.input_source)
        session_id = self.camstim_settings.session_id
        self.pkl_path = next(self.input_source.rglob("*.pkl"))
        if not self.camstim_settings.output_directory.is_dir():
            self.camstim_settings.output_directory.mkdir(parents=True)
        self.stim_table_path = (
            self.camstim_settings.output_directory
            / f"{session_id}_stim_table.csv"
        )
        self.vsync_table_path = (
            self.camstim_settings.output_directory
            / f"{session_id}_vsync_table.csv"
        )
        self.pkl_data = pkl.load_pkl(self.pkl_path)
        self.fps = pkl.get_fps(self.pkl_data)
        self.stage_name = pkl.get_stage(self.pkl_data)
        self.session_start, self.session_end = self._get_sync_times()
        self.sync_data = sync.load_sync(self.sync_path)
        self.mouse_id = self.camstim_settings.subject_id
        self.session_uuid = self.get_session_uuid()
        self.behavior = self._is_behavior()
        self.session_type = self._get_session_type()

    def _get_session_type(self) -> str:
        """Determine the session type from the pickle data

        Returns
        -------
        str
            session type
        """
        if self.behavior:
            return self.pkl_data["items"]["behavior"]["params"]["stage"]
        else:
            return self.pkl_data["items"]["foraging"]["params"]["stage"]

    def _is_behavior(self) -> bool:
        """Check if the session has behavior data"""
        if self.pkl_data.get("items", {}).get("behavior", None):
            return True
        return False

    def _get_sync_times(self) -> None:
        """Set the sync path
        Returns
        -------
        Path
        """
        self.sync_path = next(self.input_source.glob("*.h5"))
        self.sync_data = sync.load_sync(self.sync_path)
        return sync.get_start_time(self.sync_data), sync.get_stop_time(
            self.sync_data
        )

    def build_behavior_table(self) -> None:
        """Builds a behavior table from the stimulus pickle file and writes it
        to a csv file

        Returns
        -------
        None
        """
        timestamps = sync.get_ophys_stimulus_timestamps(
            self.sync_data, self.pkl_path
        )
        behavior_table = behavior_utils.from_stimulus_file(
            self.pkl_path, timestamps
        )
        behavior_table[0].to_csv(self.stim_table_path, index=False)

    def get_session_uuid(self) -> str:
        """Returns the session uuid from the pickle file"""
        return pkl.load_pkl(self.pkl_path)["session_uuid"]

    def get_stim_table_seconds(
        self, stim_table_sweeps, frame_times, name_map
    ) -> pd.DataFrame:
        """Builds a stimulus table from the stimulus pickle file, sync file

        Parameters
        ----------
        stim_table_sweeps : pd.DataFrame
            DataFrame containing stimulus information
        frame_times : np.array
            Array containing frame times
        name_map : dict
            Dictionary containing stimulus names

        Returns
        -------
        pd.DataFrame
        """
        stim_table_seconds = stim_utils.convert_frames_to_seconds(
            stim_table_sweeps, frame_times, self.fps, True
        )
        stim_table_seconds = names.collapse_columns(stim_table_seconds)
        stim_table_seconds = names.drop_empty_columns(stim_table_seconds)
        stim_table_seconds = names.standardize_movie_numbers(
            stim_table_seconds
        )
        stim_table_seconds = names.add_number_to_shuffled_movie(
            stim_table_seconds
        )
        stim_table_seconds = names.map_stimulus_names(
            stim_table_seconds, name_map
        )
        return stim_table_seconds

    def build_stimulus_table(
        self,
        minimum_spontaneous_activity_duration=0.0,
        extract_const_params_from_repr=False,
        drop_const_params=stim_utils.DROP_PARAMS,
        stimulus_name_map=constants.default_stimulus_renames,
        column_name_map=constants.default_column_renames,
        modality="ephys",
    ):
        """
        Builds a stimulus table from the stimulus pickle file, sync file, and
        the given parameters. Writes the table to a csv file.

        Parameters
        ----------
        minimum_spontaneous_activity_duration : float, optional
            Minimum duration of spontaneous activity to be considered a
            separate epoch, by default 0.0
        extract_const_params_from_repr : bool, optional
            Whether to extract constant parameters from the stimulus
            representation, by default False
        drop_const_params : list[str], optional
            List of constant parameters to drop, by default stim.DROP_PARAMS
        stimulus_name_map : dict[str, str], optional
            Map of stimulus names to rename, by default
            names.default_stimulus_renames
        column_name_map : dict[str, str], optional
            Map of column names to rename, by default
            names.default_column_renames

        """
        assert (
            not self.behavior
        ), "Can't generate regular stim table from behavior pkl. \
            Use build_behavior_table instead."

        vsync_times = stim_utils.extract_frame_times_from_vsync(self.sync_data)
        if modality == "ephys":
            frame_times = stim_utils.extract_frame_times_from_photodiode(
                self.sync_data
            )
        elif modality == "ophys":
            delay = stim_utils.extract_frame_times_with_delay(self.sync_data)
            frame_times = stim_utils.extract_frame_times_from_vsync(
                self.sync_data
            )
            frame_times = frame_times + delay
        times = [frame_times, vsync_times]

        for i, time in enumerate(times):
            minimum_spontaneous_activity_duration = (
                minimum_spontaneous_activity_duration
                / pkl.get_fps(self.pkl_data)
            )

            stimulus_table = functools.partial(
                stim_utils.build_stimuluswise_table,
                seconds_to_frames=stim_utils.seconds_to_frames,
                extract_const_params_from_repr=extract_const_params_from_repr,
                drop_const_params=drop_const_params,
            )

            spon_table = functools.partial(
                stim_utils.make_spontaneous_activity_tables,
                duration_threshold=minimum_spontaneous_activity_duration,
            )

            stimuli = pkl.get_stimuli(self.pkl_data)
            stimuli = stim_utils.extract_blocks_from_stim(stimuli)
            stim_table_sweeps = stim_utils.create_stim_table(
                self.pkl_data, stimuli, stimulus_table, spon_table
            )

            stim_table_seconds = self.get_stim_table_seconds(
                stim_table_sweeps, time, stimulus_name_map
            )
            stim_table_final = names.map_column_names(
                stim_table_seconds, column_name_map, ignore_case=False
            )
            if i == 0:
                stim_table_final.to_csv(self.stim_table_path, index=False)
            else:
                stim_table_final.to_csv(self.vsync_table_path, index=False)

    def _summarize_epoch_params(
        self,
        stim_table: pd.DataFrame,
        current_epoch: list,
        start_idx: int,
        end_idx: int,
    ):
        """
        This fills in the current_epoch tuple with the set of parameters
        that exist between start_idx and end_idx
        """
        for column in stim_table:
            if column not in (
                "start_time",
                "stop_time",
                "stim_name",
                "stim_type",
                "start_frame",
                "end_frame",
                "frame",
                "duration",
                "image_set",
                "stim_block",
                "flashes_since_change",
                "image_index",
                "is_change",
                "omitted",
            ):
                param_set = set(stim_table[column][start_idx:end_idx].dropna())
                if len(param_set) > 1000:
                    current_epoch[3][column] = ["Error: over 1000 values"]
                elif param_set:
                    current_epoch[3][column] = param_set

    def extract_stim_epochs(
        self, stim_table: pd.DataFrame
    ) -> list[list[str, int, int, dict, set]]:
        """
        Returns a list of stimulus epochs, where an epoch takes the form
        (name, start, stop, params_dict, template names). Iterates over the
        stimulus epochs table, identifying epochs based on when the
        'stim_name' field of the table changes.

        For each epoch, every unknown column (not start_time, stop_time,
        stim_name, stim_type, or frame) are listed as parameters, and the set
        of values for that column are listed as parameter values.
        """
        epochs = []

        current_epoch = [None, 0.0, 0.0, {}, set()]
        epoch_start_idx = 0
        for current_idx, row in stim_table.iterrows():
            if row["stim_name"] == "spontaneous":
                continue
            if row["stim_name"] != current_epoch[0]:
                self._summarize_epoch_params(
                    stim_table, current_epoch, epoch_start_idx, current_idx
                )
                epochs.append(current_epoch)
                epoch_start_idx = current_idx
                current_epoch = [
                    row["stim_name"],
                    row["start_time"],
                    row["stop_time"],
                    {},
                    set(),
                ]
            else:
                current_epoch[2] = row["stop_time"]

            stim_name = row.get("stim_name", "") or ""
            image_set = row.get("image_set", "")
            if pd.notnull(image_set):
                stim_name = image_set

            if "image" in stim_name.lower() or "movie" in stim_name.lower():
                current_epoch[4].add(row["stim_name"])
        return epochs[1:]

    def epochs_from_stim_table(self) -> list[StimulusEpoch]:
        """
        From the stimulus epochs table, return a list of schema stimulus
        epochs representing the various periods of stimulus from the session.
        Also include the camstim version from pickle file and stimulus script
        used from mtrain.
        """

        software_obj = Software(
            name="camstim",
            version="1.0",
            url="https://eng-gitlab.corp.alleninstitute.org/braintv/camstim",
        )

        script_obj = Software(name=self.stage_name, version="1.0")

        print("STIM PATH", self.stim_table_path)
        schema_epochs = []
        for (
            epoch_name,
            epoch_start,
            epoch_end,
            stim_params,
            stim_template_names,
        ) in self.extract_stim_epochs(pd.read_csv(self.stim_table_path)):
            params_obj = VisualStimulation(
                stimulus_name=epoch_name,
                stimulus_parameters=stim_params,
                stimulus_template_name=stim_template_names,
            )

            epoch_obj = StimulusEpoch(
                stimulus_start_time=self.session_start
                + timedelta(seconds=epoch_start),
                stimulus_end_time=self.session_start
                + timedelta(seconds=epoch_end),
                stimulus_name=epoch_name,
                software=[software_obj],
                script=script_obj,
                stimulus_modalities=[StimulusModality.VISUAL],
                stimulus_parameters=[params_obj],
            )
            schema_epochs.append(epoch_obj)

        return schema_epochs
