"""Module to define core generic models"""

import argparse
import json
import logging
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    InitSettingsSource,
    PydanticBaseSettingsSource,
)


class JobResponse(BaseModel):
    """Standard model of a JobResponse."""

    model_config = ConfigDict(extra="forbid")
    status_code: int
    message: Optional[str] = Field(None)
    data: Optional[str] = Field(None)


class JsonConfigSettingsSource(PydanticBaseSettingsSource):
    """Base class for settings that parse JSON from various sources."""

    def __init__(self, settings_cls, config_file_location: Path):
        """Class constructor."""
        self.config_file_location = config_file_location
        super().__init__(settings_cls)

    def _retrieve_contents(self) -> Dict[str, Any]:
        """Retrieve and parse the JSON contents from the config file."""
        try:
            with open(self.config_file_location, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(
                f"Error loading config from {self.config_file_location}: {e}"
            )
            raise e

    @cached_property
    def _json_contents(self):
        """Cache contents to a property to avoid re-downloading."""
        contents = self._retrieve_contents()
        return contents

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        """
        Gets the value, the key for model creation, and a flag to determine
        whether value is complex.
        Parameters
        ----------
        field : FieldInfo
          The field
        field_name : str
          The field name

        Returns
        -------
        Tuple[Any, str, bool]
          A tuple contains the key, value and a flag to determine whether
          value is complex.

        """
        file_content_json = self._json_contents
        field_value = file_content_json.get(field_name)
        return field_value, field_name, False

    def prepare_field_value(
        self,
        field_name: str,
        field: FieldInfo,
        value: Any,
        value_is_complex: bool,
    ) -> Any:
        """
        Prepares the value of a field.
        Parameters
        ----------
        field_name : str
          The field name
        field : FieldInfo
          The field
        value : Any
          The value of the field that has to be prepared
        value_is_complex : bool
          A flag to determine whether value is complex

        Returns
        -------
        Any
          The prepared value

        """
        return value

    def __call__(self) -> Dict[str, Any]:
        """
        Run this when this class is called. Required to be implemented.

        Returns
        -------
        Dict[str, Any]
          The fields for the settings defined as a dict object.

        """
        d: Dict[str, Any] = {}

        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            if field_value is not None:
                d[field_key] = field_value

        return d


class BaseJobSettings(BaseSettings):
    """Parent class for generating settings from a config file."""

    job_settings_name: str = Field(
        ...,
        description=(
            "Literal name for job settings to make serialized class distinct."
        ),
    )
    input_source: Optional[Union[Path, str, List[str], List[Path]]] = Field(
        default=None,
        description=(
            "Location or locations of data sources to parse for metadata."
        ),
    )
    output_directory: Optional[Union[Path, str]] = Field(
        default=None,
        description=(
            "Location to metadata file data to. None to return object."
        ),
    )

    user_settings_config_file: Optional[Union[Path, str]] = Field(
        default=None,
        repr=False,
        description="Optionally pull settings from a local config file.",
    )

    class Config:
        """Pydantic config to exclude field from displaying"""

        exclude = {"user_settings_config_file"}

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: InitSettingsSource,
        env_settings: EnvSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Customize the order of settings sources, including JSON file.
        """
        config_file = init_settings.init_kwargs.get(
            "user_settings_config_file"
        )
        sources = [init_settings, env_settings]

        if isinstance(config_file, str):
            config_file = Path(config_file)

        if config_file and config_file.is_file():
            sources.append(JsonConfigSettingsSource(settings_cls, config_file))

        return tuple(sources)

    @classmethod
    def from_args(cls, args: list):
        """
        Adds ability to construct settings from a list of arguments.
        Parameters
        ----------
        args : list
        A list of command line arguments to parse.
        """

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-j",
            "--job-settings",
            required=True,
            type=str,
            help=(
                r"""
                Custom settings defined by the user defined as a json
                 string. For example: -j
                 '{
                 "input_source":"/directory/to/read/from",
                 "output_directory":"/directory/to/write/to",
                 "job_settings_name": "Bergamo"}'
                """
            ),
        )
        job_args = parser.parse_args(args)
        job_settings_from_args = cls.model_validate_json(job_args.job_settings)
        return cls(
            job_settings=job_settings_from_args,
        )
