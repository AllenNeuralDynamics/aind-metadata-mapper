"""
File containing Camstim class
"""

import functools
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
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
    sessions_root: Optional[Path] = None
    opto_conditions_map: Optional[dict] = None
    overwrite_tables: bool = False
    mtrain_server: str = "http://mtrain:5000"
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
        self.session_uuid = None
        self.mtrain_regimen = None
        self.camstim_settings = camstim_settings
        self.session_path = Path(self.camstim_settings.input_source)
        session_id = self.camstim_settings.session_id
        self.pkl_path = next(self.session_path.rglob("*.pkl"))
        self.stim_table_path = (
            self.pkl_path.parent / f"{session_id}_stim_table.csv"
        )
        if self.camstim_settings.output_directory:
            self.stim_table_path = (
                self.camstim_settings.output_directory
                / f"{session_id}_behavior"
                / f"{session_id}_stim_table.csv"
            )

        self.session_start, self.session_end = self._get_sync_data()
        self.mouse_id = self.camstim_settings.subject_id
        self.session_uuid = self.get_session_uuid()
        self.mtrain_regimen = self.get_mtrain()

        self.behavior = self._is_behavior()

    def _is_behavior(self) -> bool:
        """Check if the session has behavior data"""
        return pkl.load_pkl(self.pkl_path)["items"].get("behavior", None)

    def _get_sync_data(self) -> None:
        """Set the sync path

        Returns
        -------
        Path
        """
        self.sync_path = next(self.session_path.glob("*.h5"))
        sync_data = sync.load_sync(self.sync_path)
        return sync.get_start_time(sync_data), sync.get_stop_time(sync_data)

    def build_behavior_table(self):
        stim_file = self.pkl_path
        sync_file = sync.load_sync(self.sync_path)
        timestamps = sync.get_ophys_stimulus_timestamps(sync_file, stim_file)
        behavior_table = behavior_utils.from_stimulus_file(
            stim_file, timestamps
        )
        behavior_table[0].to_csv(self.stim_table_path, index=False)

    def get_session_uuid(self) -> str:
        """Returns the session uuid from the pickle file"""
        return pkl.load_pkl(self.pkl_path)["session_uuid"]

    def get_mtrain(self) -> dict:
        """Returns dictionary containing 'id', 'name', 'stages', 'states'"""
        server = self.camstim_settings.mtrain_server
        req = f"{server}/behavior_session/{self.session_uuid}/details"
        mtrain_response = requests.get(req).json()
        return mtrain_response["result"]["regimen"]

    def build_stimulus_table(
        self,
        minimum_spontaneous_activity_duration=0.0,
        extract_const_params_from_repr=False,
        drop_const_params=stim_utils.DROP_PARAMS,
        stimulus_name_map=constants.default_stimulus_renames,
        column_name_map=constants.default_column_renames,
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
        stim_file = pkl.load_pkl(self.pkl_path)
        sync_file = sync.load_sync(self.sync_path)

        frame_times = stim_utils.extract_frame_times_from_photodiode(sync_file)
        minimum_spontaneous_activity_duration = (
            minimum_spontaneous_activity_duration / pkl.get_fps(stim_file)
        )

        stimulus_tabler = functools.partial(
            stim_utils.build_stimuluswise_table,
            seconds_to_frames=stim_utils.seconds_to_frames,
            extract_const_params_from_repr=extract_const_params_from_repr,
            drop_const_params=drop_const_params,
        )

        spon_tabler = functools.partial(
            stim_utils.make_spontaneous_activity_tables,
            duration_threshold=minimum_spontaneous_activity_duration,
        )

        stimuli = pkl.get_stimuli(stim_file)
        stimuli = stim_utils.extract_blocks_from_stim(stimuli)
        stim_table_sweeps = stim_utils.create_stim_table(
            stim_file, stimuli, stimulus_tabler, spon_tabler
        )

        stim_table_seconds = stim_utils.convert_frames_to_seconds(
            stim_table_sweeps, frame_times, pkl.get_fps(stim_file), True
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
            stim_table_seconds, stimulus_name_map
        )

        stim_table_final = names.map_column_names(
            stim_table_seconds, column_name_map, ignore_case=False
        )

        stim_table_final.to_csv(self.stim_table_path, index=False)

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
            # if the stim name changes, summarize current epoch's parameters
            # and start a new epoch
            if row["stim_name"] != current_epoch[0]:
                for column in stim_table:
                    if column not in (
                        "start_time",
                        "stop_time",
                        "stim_name",
                        "stim_type",
                        "frame",
                    ):
                        param_set = set(
                            stim_table[column][
                                epoch_start_idx:current_idx
                            ].dropna()
                        )
                        current_epoch[3][column] = param_set

                epochs.append(current_epoch)
                epoch_start_idx = current_idx
                current_epoch = [
                    row["stim_name"],
                    row["start_time"],
                    row["stop_time"],
                    {},
                    set(),
                ]
            # if stim name hasn't changed, we are in the same epoch, keep
            # pushing the stop time
            else:
                current_epoch[2] = row["stop_time"]

            # if this row is a movie or image set, record it's stim name in
            # the epoch's templates entry
            stim_name = row.get("stim_name", "")
            if pd.isnull(stim_name):
                stim_name = ""

            if "image" in stim_name.lower() or "movie" in stim_name.lower():
                current_epoch[4].add(row["stim_name"])

        # slice off dummy epoch from beginning
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

        script_obj = Software(
            name=self.mtrain_regimen["name"],
            version="1.0",
            url=self.mtrain_regimen["script"],
        )

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


class OpenEphysCamstim(Camstim):
    """stimulus data generation for open ephys data"""

    def __init__(self, camstim_settings: CamstimSettings):
        """initialize open ephys camstim object

        Parameters
        ----------
        camstim_settings : CamstimSettings
           settings for camstim object
        """
        self.camstim_settings = camstim_settings
        if (
            not self.stim_table_path.exists()
            or self.camstim_settings.overwrite_tables
        ):
            print("building stim table")
            self.build_stimulus_table()

        self.mouse_id = self.camstim_settings.subject_id
        self.session_uuid = self.get_session_uuid()
        self.mtrain_regimen = self.get_mtrain()

        if (
            not self.stim_table_path.exists()
            or self.camstim_settings["overwrite_tables"]
        ):
            print("building stim table")
            self.build_stimulus_table()

        sync_data = sync.load_sync(self.sync_path)
        self.session_start = sync.get_start_time(sync_data)
        self.session_end = sync.get_stop_time(sync_data)

        pkl_data = pkl.load_pkl(self.pkl_path)
        if pkl_data["items"].get("behavior", None):
            self.build_behavior_table()
        else:
            self.build_stimulus_table()

        print("getting stim epochs")
        self.stim_epochs = self.epochs_from_stim_table()
        input_source = Path(self.camstim_settings.get("input_source"))
        session_id = self.camstim_settings.session_id
        if self.camstim_settings.opto_conditions_map is None:
            self.opto_conditions_map = names.DEFAULT_OPTO_CONDITIONS
        else:
            self.opto_conditions_map = (
                self.camstim_settings.opto_conditions_map
            )
        self.session_path = self.get_session_path(session_id, input_source)
        self.folder = self.get_folder(session_id, input_source)
        self.opto_pkl_path = self.session_path / f"{self.folder}.opto.pkl"
        self.opto_table_path = (
            self.session_path / f"{self.folder}_opto_epochs.csv"
        )
        self.pkl_path = self.session_path / f"{self.folder}.stim.pkl"

        self.stim_table_path = (
            self.session_path / f"{self.folder}_stim_epochs.csv"
        )
        self.sync_path = self.session_path / f"{self.folder}.sync"

        if (
            self.opto_pkl_path.exists()
            and not self.opto_table_path.exists()
            or self.camstim_settings.overwrite_tables
        ):
            print("building opto table")
            self.build_optogenetics_table()
        self.build_stimulus_table()

    def get_folder(self, session_id, input_source) -> str:
        """returns the directory name of the session on the np-exp directory"""
        for subfolder in input_source.iterdir():
            if subfolder.name.split("_")[0] == session_id:
                return subfolder.name
        else:
            raise Exception("Session folder not found in np-exp")

    def get_session_path(self, session_id, input_source) -> Path:
        """returns the path to the session on allen's  directory"""
        return input_source / self.get_folder(session_id, input_source)

    def epoch_from_opto_table(self) -> StimulusEpoch:
        """
        From the optogenetic stimulation table, returns a single schema
        stimulus epoch representing the optotagging period. Include all
        unknown table columns (not start_time, stop_time, stim_name) as
        parameters, and include the set of all of that column's values as the
        parameter values.
        """

        script_obj = Software(
            name=self.mtrain_regimen["name"],
            version="1.0",
            url=self.mtrain_regimen,
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
