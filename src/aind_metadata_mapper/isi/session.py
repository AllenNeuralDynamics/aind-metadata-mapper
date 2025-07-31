"""Mesoscope ETL"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Union

import h5py as h5
from aind_data_schema.core.session import Session, StimulusEpoch, Stream, StimulusModality
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.isi.models import JobSettings


class ISI(GenericEtl[JobSettings]):
    """Class to manage transforming ISI platform json and metadata into
    a Session model."""

    def __init__(self, job_settings: Union[JobSettings, str]):
        """
        Class constructor for Base etl class.
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
        self.trial_files = self.get_trial_files()
        self.start_time, self.end_time = self.get_start_end_times()

    def get_trial_files(self) -> List[Path]:
        """Gets the trial files from the input source directory and sorts them
        by their creation time.

        Returns
        -------
        List[Path]
            A list of sorted trial file paths.
        """
        trials = list(self.job_settings.input_source.glob("*trial*.hdf5"))
        if not trials:
            raise ValueError("No trials found in the input source directory.")
        trials.sort(
            key=lambda x: x.stat().st_ctime
        )  # Trials contain <unique_id>_trial<trial_number>
        return trials

    def get_start_end_times(self) -> Tuple[datetime, datetime]:
        """Grabs the start and end times from the first trials
        creation time and the last trials completion time

        Returns
        -------
        tuple
            (start_time: datetime, end_time: datetime)
        """
        start_time = datetime.fromtimestamp(
            self.trial_files[0].stat().st_ctime
        )
        end_time = datetime.fromtimestamp(self.trial_files[-1].stat().st_ctime)
        return start_time, end_time

    def _extract(self) -> List[StimulusEpoch]:
        """Extracts the session and modality from the job settings.

        Returns
        -------
        List[StimulusEpoch]
            A list of stimulus epochs extracted from the trial files.
        """
        stimulus_epochs = []
        for trial in self.trial_files:
            with h5.File(trial, "r") as f:
                trial_times = f["raw_images_timestamp"][()]
                trial_start = datetime.fromtimestamp(trial.stat().st_ctime)
                trial_end = datetime.fromtimestamp(
                    trial_start.timestamp() + trial_times[-1]
                )
                stim_epoch = StimulusEpoch(
                    stimulus_start_time=trial_start,
                    stimulus_end_time=trial_end,
                    stimulus_name=trial.name.split(".hdf5")[0],  # Use the file name without extension
                    stimulus_modalities=[StimulusModality.VISUAL]
                )
            stimulus_epochs.append(stim_epoch)

        return stimulus_epochs

    def _transform(self, stimulus_epochs: List[StimulusEpoch]) -> None:
        """Transforms the job settings into a Session model.

        Parameters
        ----------
        stimulus_epochs: List[StimulusEpoch]
            A list of stimulus epochs to transform.
        Returns
        -------
        Session
            A Session object containing the transformed data.
        """

        # Create the data stream
        data_streams = [
            Stream(
                stream_start_time=self.start_time,
                stream_end_time=self.end_time,
                camera_names=["Light source goes here XXX"],
                stream_modalities=[Modality.ISI]

            )
        ]
        return Session(
            session_start_time=self.start_time,
            session_end_time=self.end_time,
            experimenter_full_name=self.job_settings.experimenter_full_name,
            subject_id=self.job_settings.subject_id,
            data_streams=data_streams,
            stimulus_epochs=stimulus_epochs,
            session_type="ISI",
            rig_id="ISI.1",
            mouse_platform_name="disc",
            active_mouse_platform=True,
        )

        
    def run_job(self) -> None:
        """Loads the session into the database."""
        # Here you would implement the logic to save the session to your database
        # For example, using an ORM or direct database connection
        epoch_data = self._extract()
        transformed = self._transform(epoch_data)
        transformed.write_standard_file(
            output_directory=self.job_settings.output_directory
        )
        logging.info(
            f"Session loaded successfully."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ISI ETL job")
    parser.add_argument(
        "--input-source",
        type=Path,
        required=True,
        help="Path to the input source directory containing trial files",
    )
    parser.add_argument(
        "--experimenter-full-name",
        type=str,
        nargs="+",
        default=["unknown user"],
        help="Full name of the experimenter",
    )
    parser.add_argument(
        "--subject-id",
        type=str,
        required=True,
        help="Subject ID for the session",
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path("."),
        help="Directory to save the output session data",
    )
    args = parser.parse_args()

    job_settings = JobSettings(
        input_source=args.input_source,
        experimenter_full_name=["unknown user"],
        subject_id="unknown_subject",
    )

    isi_etl = ISI(job_settings)
    isi_etl.run_job()
