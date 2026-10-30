"""Individual mappers should be registered here by name

Each JobClass must inherit from MapperJob in base.py and accept a JobSettings subclass
"""

from aind_metadata_mapper.fip.mapper import FIPMapper
from aind_metadata_mapper.mesoscope.mapper import MesoscopeMapper

registry = {
    "fip": FIPMapper,
    "mesoscope": MesoscopeMapper,
}
