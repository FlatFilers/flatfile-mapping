"""
Flatfile Mapping in Python
"""
__version__ = "0.1.3"

from flatfile_mapping.mapping_rule import parse, MappingRule, Row
from flatfile_mapping.mapping_program import MappingProgram

__all__ = [
    "MappingRule",
    "MappingProgram",
    "Row",
    # parse a JSON representation into a typed mapping rule object
    "parse",
]
