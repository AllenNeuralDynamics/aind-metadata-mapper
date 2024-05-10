"""Shared utilities"""

import logging
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Generator, List, Tuple, Union
from xml.etree import ElementTree

import yaml  # type: ignore

logger = logging.getLogger(__name__)


def find_elements(
    et: ElementTree.Element, name: str
) -> Generator[ElementTree.Element, None, None]:
    """Find elements in an ElementTree.Element that match a name.

    Notes
    -----
    - Name matches on tags are case-insensitive match on tags for
     convenience
    """
    for element in et.iter():
        if element.tag.lower() == name.lower():
            yield element


def load_xml(xml_path: Path) -> ElementTree.Element:
    """Load xml file from path."""
    return ElementTree.fromstring(xml_path.read_text())


def load_config(config_path: Path) -> ConfigParser:
    """Load .ini file from path."""
    config = ConfigParser()
    config.read(config_path)
    return config


def load_yaml(yaml_path: Path) -> dict:
    """Load yaml file from path."""
    return yaml.safe_load(yaml_path.read_text())


def find_update(
    items: List[Any],
    filters: List[Tuple[str, Any]],
    setter=lambda item, name, value: setattr(item, name, value),
    **updates: Any,
) -> Union[Any, None]:
    """Find an item in a list of items that matches the filters and update it.
     Only the first item that matches the filters is updated.

    Notes
    -----
    - Filters are property name, property value pairs.
    """
    for item in items:
        if all(
            getattr(item, prop_name, None) == prop_value
            for prop_name, prop_value in filters
        ):
            for prop_name, prop_value in updates.items():
                setter(item, prop_name, prop_value)
            return item
    else:
        logger.debug("Failed to find matching item. filters: %s" % filters)
        return None
