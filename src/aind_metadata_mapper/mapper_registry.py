"""Individual mappers should be registered here by name

Each JobClass must inherit from MapperJob in base.py and accept a JobSettings subclass
"""

from aind_metadata_mapper.fip.mapper import FIPMapper

registry = {
    "fip": FIPMapper,
}
