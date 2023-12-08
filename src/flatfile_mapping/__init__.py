"""
Flatfile Mapping in Python
"""
__version__ = "0.0.2"

from flatfile_mapping.mapping_rule import parse, MappingRule
from flatfile_mapping.mapping_program import MappingProgram

__all__ = [
    "MappingRule",
    "MappingProgram",
    # parse a JSON representation into a typed mapping rule object
    "parse",
]
