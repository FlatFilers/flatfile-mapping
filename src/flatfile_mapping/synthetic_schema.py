from __future__ import annotations

from typing import List, Union, Literal
import re
from copy import deepcopy

from pydantic import BaseModel, Field

from flatfile_mapping.nesting import distill_groups, find_groups, make_nesting_rules


# This is not as good as the TypeScript version
class BaseFlatfileProperty(BaseModel):
    type: str


class BaseArrayProperty(BaseModel):
    type: Literal["array"]
    base: "BaseProperty"


class BaseSchemaProperty(BaseModel):
    type: Literal["schema"]
    schema_: "SyntheticSchema" = Field(alias="schema")


BaseProperty = Union[BaseFlatfileProperty, BaseArrayProperty, BaseSchemaProperty]


class FlatfileProperty(BaseFlatfileProperty):
    key: str


class ArrayProperty(BaseArrayProperty):
    key: str


class SchemaProperty(BaseSchemaProperty):
    key: str


# A property in a SyntheticSchema is either
# * a Flatfile Property (but just the key and type)
# * a SchemaProperty
# * an ArrayProperty
SyntheticProperty = Union[FlatfileProperty, ArrayProperty, SchemaProperty]


class SyntheticSchema(BaseModel):
    fields: List[SyntheticProperty]


SyntheticSchema.model_rebuild()


# Given a list of field names, create a SyntheticSchema that includes shallow nesting.
# TODO: should we give an option to disable nesting?
# TODO: should we give an option to do further nesting?
def schema_from_field_names(field_names: List[str]) -> SyntheticSchema:
    # do nesting
    groups = find_groups(field_names)
    distilled = distill_groups(groups)
    rules = make_nesting_rules(distilled)

    schema = SyntheticSchema(fields=[])

    for rule in rules:
        # Start with flatfile properties
        fields: List[SyntheticProperty] = [
            FlatfileProperty(key=subfield.destinationSubfield, type="string")
            for subfield in rule.subfields
        ]

        fields.append(FlatfileProperty(key="__control", type="string"))

        schema.fields.append(
            ArrayProperty(
                type="array",
                key=rule.destinationField,
                base=SchemaProperty(
                    type="schema",
                    key=rule.destinationField,
                    schema=SyntheticSchema(fields=fields),
                ),
            )
        )

        # Remove all these rules as their own fields
        for subfield in rule.subfields:
            field_names = [
                field_name
                for field_name in field_names
                if not re.match(subfield.sourceRegex, field_name)
            ]

    # Now add all the remaining fields
    for field_name in field_names:
        schema.fields.append(FlatfileProperty(key=field_name, type="string"))

    return schema


def combine_fields(
    schema: SyntheticSchema, field_name: str, regex: str
) -> SyntheticSchema:
    combined = deepcopy(schema)
    found = False

    base = SchemaProperty(
        type="schema",
        key=field_name,
        schema=SyntheticSchema(
            fields=[
                FlatfileProperty(key="__control", type="string"),
            ]
        ),
    )

    combined_field = ArrayProperty(
        type="array",
        key=field_name,
        base=base,
    )

    for field in combined.fields:
        match = re.match(regex, field.key)

        if match:
            if field.type != "array":
                raise Exception("Cannot combine non-array fields")

            found = True

            # if there's a regex capture use it, otherwise use the field name
            subfield = match.groups()[0] if match.groups() else field.key

            base.schema_.fields.append(FlatfileProperty(key=subfield, type="string"))

            # this seems wrong
            combined.fields = [f for f in combined.fields if f.key != field.key]

    if found:
        combined.fields.append(combined_field)

    return combined
