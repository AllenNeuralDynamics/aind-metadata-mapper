"""
File containing Camstim class
"""

import datetime
import functools

import aind_data_schema
import aind_data_schema.core.session as session_schema
import np_session
import pandas as pd

import aind_metadata_mapper.open_ephys.utils.constants as constants
import aind_metadata_mapper.open_ephys.utils.naming_utils as names
import aind_metadata_mapper.open_ephys.utils.pkl_utils as pkl
import aind_metadata_mapper.open_ephys.utils.stim_utils as stim
import aind_metadata_mapper.open_ephys.utils.sync_utils as sync


class Camstim:
    """
    Methods used to extract stimulus epochs
    """

    def __init__(
        self,
        session_id: str,
        json_settings: dict,
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
        if json_settings.get("opto_conditions_map", None) is None:
            self.opto_conditions_map = names.DEFAULT_OPTO_CONDITIONS
        else:
            self.opto_conditions_map = json_settings["opto_conditions_map"]
        overwrite_tables = json_settings.get("overwrite_tables", False)

        self.json_settings = json_settings
        session_inst = np_session.Session(session_id)
        self.mtrain = session_inst.mtrain
        self.npexp_path = session_inst.npexp_path
        self.folder = session_inst.folder

        self.pkl_path = self.npexp_path / f"{self.folder}.stim.pkl"
        self.opto_pkl_path = self.npexp_path / f"{self.folder}.opto.pkl"
        self.opto_table_path = (
            self.npexp_path / f"{self.folder}_opto_epochs.csv"
        )
        self.stim_table_path = (
            self.npexp_path / f"{self.folder}_stim_epochs.csv"
        )
        self.sync_path = self.npexp_path / f"{self.folder}.sync"

        sync_data = sync.load_sync(self.sync_path)
        self.session_start = sync.get_start_time(sync_data)
        self.session_end = sync.get_stop_time(sync_data)
        print(
            "session start : session end\n",
            self.session_start,
            ":",
            self.session_end,
        )

        if not self.stim_table_path.exists() or overwrite_tables:
            print("building stim table")
            self.build_stimulus_table()
        if (
            self.opto_pkl_path.exists()
            and not self.opto_table_path.exists()
            or overwrite_tables
        ):
            print("building opto table")
            self.build_optogenetics_table()

        print("getting stim epochs")
        self.stim_epochs = self.epochs_from_stim_table()
        if self.opto_table_path.exists():
            self.stim_epochs.append(self.epoch_from_opto_table())

    def build_stimulus_table(
        self,
        minimum_spontaneous_activity_duration=0.0,
        extract_const_params_from_repr=False,
        drop_const_params=stim.DROP_PARAMS,
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

        frame_times = stim.extract_frame_times_from_photodiode(sync_file)
        minimum_spontaneous_activity_duration = (
            minimum_spontaneous_activity_duration / pkl.get_fps(stim_file)
        )

        stimulus_tabler = functools.partial(
            stim.build_stimuluswise_table,
            seconds_to_frames=stim.seconds_to_frames,
            extract_const_params_from_repr=extract_const_params_from_repr,
            drop_const_params=drop_const_params,
        )

        spon_tabler = functools.partial(
            stim.make_spontaneous_activity_tables,
            duration_threshold=minimum_spontaneous_activity_duration,
        )

        stimuli = pkl.get_stimuli(stim_file)
        stimuli = stim.extract_blocks_from_stim(stimuli)
        stim_table_sweeps = stim.create_stim_table(
            stim_file, stimuli, stimulus_tabler, spon_tabler
        )

        stim_table_seconds = stim.convert_frames_to_seconds(
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

    def build_optogenetics_table(self, keys=stim.OPTOGENETIC_STIMULATION_KEYS):
        """
        Builds an optogenetics table from the opto pickle file and sync file.
        Writes the table to a csv file.

        Parameters
        ----------
        output_opto_table_path : str
            Path to write the optogenetics table to.
        keys : list[str], optional
            List of laser keys

        returns
        -------
        dict
            Dictionary containing the path to the output opto table
        """
        opto_file = pkl.load_pkl(self.opto_pkl_path)
        sync_file = sync.load_sync(self.sync_path)

        start_times = sync.extract_led_times(sync_file, keys)

        conditions = [str(item) for item in opto_file["opto_conditions"]]
        levels = opto_file["opto_levels"]
        assert len(conditions) == len(levels)
        if len(start_times) > len(conditions):
            raise ValueError(
                f"there are {len(start_times) - len(conditions)} extra "
                f"optotagging sync times!"
            )
        optotagging_table = pd.DataFrame(
            {
                "start_time": start_times,
                "condition": conditions,
                "level": levels,
            }
        )
        optotagging_table = optotagging_table.sort_values(
            by="start_time", axis=0
        )

        stop_times = []
        names = []
        conditions = []
        for _, row in optotagging_table.iterrows():
            condition = self.opto_conditions_map[row["condition"]]
            stop_times.append(row["start_time"] + condition["duration"])
            names.append(condition["name"])
            conditions.append(condition["condition"])

        optotagging_table["stop_time"] = stop_times
        optotagging_table["stimulus_name"] = names
        optotagging_table["condition"] = conditions
        optotagging_table["duration"] = (
            optotagging_table["stop_time"] - optotagging_table["start_time"]
        )

        optotagging_table.to_csv(self.opto_table_path, index=False)

    def epoch_from_opto_table(self) -> session_schema.StimulusEpoch:
        """
        From the optogenetic stimulation table, returns a single schema
        stimulus epoch representing the optotagging period. Include all
        unknown table columns (not start_time, stop_time, stim_name) as
        parameters, and include the set of all of that column's values as the
        parameter values.
        """
        stim = aind_data_schema.core.session.StimulusModality

        script_obj = aind_data_schema.components.devices.Software(
            name=self.mtrain["regimen"]["name"],
            version="1.0",
            url=self.mtrain["regimen"]["script"],
        )

        opto_table = pd.read_csv(self.opto_table_path)

        opto_params = {}
        for column in opto_table:
            if column in ("start_time", "stop_time", "stim_name"):
                continue
            param_set = set(opto_table[column].dropna())
            opto_params[column] = param_set

        params_obj = session_schema.VisualStimulation(
            stimulus_name="Optogenetic Stimulation",
            stimulus_parameters=opto_params,
            stimulus_template_name=[],
        )

        opto_epoch = session_schema.StimulusEpoch(
            stimulus_start_time=self.session_start
            + datetime.timedelta(seconds=opto_table.start_time.iloc[0]),
            stimulus_end_time=self.session_start
            + datetime.timedelta(seconds=opto_table.start_time.iloc[-1]),
            stimulus_name="Optogenetic Stimulation",
            software=[],
            script=script_obj,
            stimulus_modalities=[stim.OPTOGENETICS],
            stimulus_parameters=[params_obj],
        )

        return opto_epoch

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

    def epochs_from_stim_table(self) -> list[session_schema.StimulusEpoch]:
        """
        From the stimulus epochs table, return a list of schema stimulus
        epochs representing the various periods of stimulus from the session.
        Also include the camstim version from pickle file and stimulus script
        used from mtrain.
        """
        stim = aind_data_schema.core.session.StimulusModality

        software_obj = aind_data_schema.components.devices.Software(
            name="camstim",
            version=pkl.load_pkl(self.pkl_path)["platform"]["camstim"].split(
                "+"
            )[0],
            url="https://eng-gitlab.corp.alleninstitute.org/braintv/camstim",
        )

        script_obj = aind_data_schema.components.devices.Software(
            name=self.mtrain["regimen"]["name"],
            version="1.0",
            url=self.mtrain["regimen"]["script"],
        )

        schema_epochs = []
        for (
            epoch_name,
            epoch_start,
            epoch_end,
            stim_params,
            stim_template_names,
        ) in self.extract_stim_epochs(pd.read_csv(self.stim_table_path)):
            params_obj = session_schema.VisualStimulation(
                stimulus_name=epoch_name,
                stimulus_parameters=stim_params,
                stimulus_template_name=stim_template_names,
            )

            epoch_obj = session_schema.StimulusEpoch(
                stimulus_start_time=self.session_start
                + datetime.timedelta(seconds=epoch_start),
                stimulus_end_time=self.session_start
                + datetime.timedelta(seconds=epoch_end),
                stimulus_name=epoch_name,
                software=[software_obj],
                script=script_obj,
                stimulus_modalities=[stim.VISUAL],
                stimulus_parameters=[params_obj],
            )
            schema_epochs.append(epoch_obj)

        return schema_epochs
