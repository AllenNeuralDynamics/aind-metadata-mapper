"""ISI ETL"""

import argparse
import logging
import os
from pathlib import Path
from typing import List, Union

from aind_data_schema.core.session import (
    LightEmittingDiodeConfig,
    Session,
    StimulusEpoch,
    StimulusModality,
    Stream,
    VisualStimulation,
)
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

    def _extract(self) -> List[StimulusEpoch]:
        """Extracts the session and modality from the job settings.

        Returns
        -------
        List[StimulusEpoch]
            A list of stimulus epochs extracted from the trial files.
        """
        # Create visual stimulation parameters
        visual_stim_params = VisualStimulation(
            stimulus_name="DriftingCheckerboardBar",
            stimulus_parameters={
                "window": {
                    "size": [1920, 1200],
                    "monitor": "testMonitor",
                    "fullscr": True,
                    "color": [0, 0, 0],
                    "screen": 2
                },
                "mask": None,
                "units": "pix",
                "pos": [0, 0],
                "size": [1920, 1200],
                "ori": [0, 90, 180, 270],
                "color": [1, 1, 1],
                "colorSpace": "rgb",
                "contrast": 1.0,
                "opacity": 1.0,
                "depth": 0,
                "interpolate": False,
                "flipHoriz": False,
                "flipVert": False,
                "texRes": 256,
                "spherical": True
            }
        )
        stimulus_epoch = [
            StimulusEpoch(
                stimulus_start_time=self.job_settings.session_start_time,
                stimulus_end_time=self.job_settings.session_end_time,
                stimulus_name="DriftingCheckerboardBar",
                stimulus_modalities=[StimulusModality.VISUAL],
                stimulus_parameters=[visual_stim_params]
            )
        ]

        return stimulus_epoch

    def _transform(self, stimulus_epochs: List[StimulusEpoch]) -> Session:
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
                stream_start_time=self.job_settings.session_start_time,
                stream_end_time=self.job_settings.session_end_time,
                light_sources=[
                    LightEmittingDiodeConfig(
                        name="ISI LED",
                    )
                ],
                stream_modalities=[Modality.ISI],
            )
        ]
        return Session(
            session_start_time=self.job_settings.session_start_time,
            session_end_time=self.job_settings.session_end_time,
            experimenter_full_name=self.job_settings.experimenter_full_name,
            subject_id=self.job_settings.subject_id,
            data_streams=data_streams,
            stimulus_epochs=stimulus_epochs,
            session_type="ISI",
            rig_id=os.getenv("aibs_rig_id", ""),
            mouse_platform_name="disc",
            active_mouse_platform=True,
        )

    def run_job(self) -> None:
        """Loads the session into the database."""
        epoch_data = self._extract()
        transformed = self._transform(epoch_data)
        transformed.write_standard_file(
            output_directory=self.job_settings.output_directory
        )
        logging.info("Session loaded successfully.")


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
