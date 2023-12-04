from pydantic import BaseModel, Field
from typing import Union, List, Dict, Literal, Annotated

# Corresponds to the TypeScript Value type
# TODO: can we get a better type on the list?
Value = Union[str, int, float, bool, None, list]


# Corresponds to the TypeScript Row interface
Row = Dict[str, Union[Value, "Row", List["Row"]]]


class BaseMappingRule(BaseModel):
    filter: str = Field(default=None)
    name: str = Field(default=None)
    description: str = Field(default=None)


class Assign(BaseMappingRule):
    type: Literal["assign"]
    sourceField: str
    destinationField: str


class Ignore(BaseMappingRule):
    type: Literal["ignore"]
    sourceField: str


class Constant(BaseMappingRule):
    type: Literal["constant"]
    destinationField: str
    value: Value


class Transform(BaseMappingRule):
    type: Literal["transform"]
    sourceField: str
    destinationField: str
    transform: Literal["uppercase", "lowercase"]


class RegexExtract(BaseMappingRule):
    type: Literal["regex-extract"]
    sourceField: str
    regex: str
    destinationFields: List[str]


class Interpolate(BaseMappingRule):
    type: Literal["interpolate"]
    sourceFields: List[str]
    destinationField: str
    output: str


class Arithmetic(BaseMappingRule):
    type: Literal["arithmetic"]
    equation: str
    sourceFields: List[str]
    destinationField: str


class Delete(BaseMappingRule):
    type: Literal["delete"]
    destinationField: str


class Subprogram(BaseMappingRule):
    type: Literal["subprogram"]
    rules: List["MappingRule"]


class Coalesce(BaseMappingRule):
    type: Literal["coalesce"]
    sourceFields: List[str]
    destinationField: str
    defaultValue: Value = Field(default=None)


class Concatenate(BaseMappingRule):
    type: Literal["concatenate"]
    sourceFields: List[str]
    destinationField: str
    separator: str = Field(default=None)


class Array(BaseMappingRule):
    type: Literal["array"]
    sourceFields: List[str]
    destinationField: str


class NestSubfield(BaseModel):
    sourceRegex: str
    destinationSubfield: str


# Explicit nesting: e.g. "all fields that match this regex go into this field"
class Nest(BaseMappingRule):
    type: Literal["nest"]
    subfields: List[NestSubfield]
    destinationField: str


class FindReplacePair(BaseModel):
    find: str
    replace: str


class FindReplace(BaseMappingRule):
    type: Literal["find-replace"]
    destinationField: str
    values: List[FindReplacePair]


# As MappingRule has recursive references, define it after all other classes
MappingRule = Annotated[
    Union[
        Assign,
        Ignore,
        Constant,
        Transform,
        RegexExtract,
        Interpolate,
        Arithmetic,
        Delete,
        Subprogram,
        Coalesce,
        Concatenate,
        Array,
        Nest,
        FindReplace,
    ],
    Field(discriminator="type"),
]


def parse(obj: dict) -> MappingRule:
    rule_type = obj["type"]
    if rule_type == "assign":
        return Assign.model_validate(obj)
    elif rule_type == "ignore":
        return Ignore.model_validate(obj)
    elif rule_type == "constant":
        return Constant.model_validate(obj)
    elif rule_type == "transform":
        return Transform.model_validate(obj)
    elif rule_type == "regex-extract":
        return RegexExtract.model_validate(obj)
    elif rule_type == "interpolate":
        return Interpolate.model_validate(obj)
    elif rule_type == "arithmetic":
        return Arithmetic.model_validate(obj)
    elif rule_type == "delete":
        return Delete.model_validate(obj)
    elif rule_type == "subprogram":
        return Subprogram.model_validate(obj)
    elif rule_type == "coalesce":
        return Coalesce.model_validate(obj)
    elif rule_type == "concatenate":
        return Concatenate.model_validate(obj)
    elif rule_type == "array":
        return Array.model_validate(obj)
    elif rule_type == "nest":
        return Nest.model_validate(obj)
    elif rule_type == "find-replace":
        return FindReplace.model_validate(obj)

    raise ValueError(f"Unknown rule type: {rule_type}")
