from typing import List, Dict, Any, Optional, cast
import re
from functools import cmp_to_key

import pandas as pd
import numpy as np
from sympy import parse_expr

from flatfile_mapping.mapping_rule import (
    Assign,
    Constant,
    FindReplace,
    Ignore,
    MappingRule,
    Row,
    Transform,
    Value,
    parse,
)
from flatfile_mapping.filter import Filter


def as_number(s: Any) -> Optional[int]:
    try:
        return int(float(s) * 1_000)
    except ValueError:
        return None


def as_string(s: pd.Series) -> pd.Series:
    return s.where(pd.notna(s), "").astype(str)


def cmp_by_control(a: dict, b: dict) -> int:
    """
    Compare two records, a and b, by their __control field.
    """
    a_control = a.get("__control")
    b_control = b.get("__control")

    a_num = as_number(a_control)
    b_num = as_number(b_control)
    if a_num is not None and b_num is not None:
        return a_num - b_num
    elif a_num is not None:
        return -1
    elif b_num is not None:
        return 1
    elif str(a_control) == str(b_control):
        return 0
    else:
        return 1 if str(a) > str(b) else -1


# Will use this to sort nested records by their control field
by_control = cmp_to_key(cmp_by_control)


def num_values(d: dict) -> int:
    return sum(v is not None for v in d.values())


class MappingProgram:
    def __init__(self, rules: List[MappingRule]):
        self.rules = rules

        # Pre-parse the filters so we only have to do it once
        self.filters = [
            Filter.from_query(rule.filter) if rule.filter else None for rule in rules
        ]

    @staticmethod
    def from_json(json_rules: List[Dict[str, Any]]) -> "MappingProgram":
        rules = [parse(rule) for rule in json_rules]
        return MappingProgram(rules)

    def run(self, records: List[Row]) -> List[Row]:
        """
        Run the mapping program over a list of records.
        Don't return empty records.
        """
        transformed = [self.run_one(record) for record in records]

        return [record for record in transformed if num_values(record) > 0]

    def run_df(
        self,
        records: pd.DataFrame,
        transformed: Optional[pd.DataFrame] = None,
        start_idxs: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        Run the mapping program over a pandas dataframe of records, producing an output
        dataframe. We may call this recursively if we have subprograms, which is why
        we might pass in an existing transformed dataframe and also a series of
        Trues and Falses corresponding to a "filter" already applied.
        """
        # If we don't have a transformed dataframe, create one that's empty
        # and has the same index as the records.
        transformed = (
            transformed
            if transformed is not None
            else pd.DataFrame(index=records.index)
        )

        # If we don't have start idxs then they're everything
        start_idxs = (
            start_idxs
            if start_idxs is not None
            else pd.Series(True, index=records.index)
        )

        def get_value(field_name: str, idxs: pd.Series) -> pd.Series:
            """
            Get the series with the given name. If it starts with 'destination!' then
            get it from the transformed dataframe, otherwise get it from the records.
            """
            # Case 1: we're getting a field from the transformed dataframe
            if field_name.startswith("destination!"):
                field_name = field_name.replace("destination!", "")
                if field_name in transformed.columns:
                    return transformed.loc[idxs, field_name]
                else:
                    # Return a series of Nones with the same index as records[idxs].
                    # We do this in sort of a convoluted way.
                    # idxs is a series of True/False values.
                    # We reindex it to the index of records, replacing True with
                    # None and False with np.nan. Then by calling dropna we get
                    # rid of all the false and have a series of Nones whose index
                    # is the same as records[idxs].
                    return (
                        idxs.reindex(records.index)
                        .replace({False: np.nan})
                        .dropna()
                        .replace({True: None})
                    )
            # Case 2: we're getting a field from the records dataframe
            else:
                if field_name in records.columns:
                    return records.loc[idxs, field_name]
                else:
                    # return a series of Nones

                    return (
                        idxs.reindex(records.index)
                        .replace({False: np.nan})
                        .dropna()
                        .replace({True: None})
                    )

        def set_value(
            field_name: str,
            value: Any,
            idxs: pd.Series,
        ):
            """
            Set the given column to the given value. If it starts with 'source!'
            then set it in the records dataframe, otherwise set it in the transformed
            dataframe.
            """
            if field_name.startswith("source!"):
                field_name = field_name.replace("source!", "")

                records.loc[idxs, field_name] = value

            else:
                transformed.loc[idxs, field_name] = value

        for rule, filter in zip(self.rules, self.filters):
            # If there's a filter, apply it and combined it with the start_idxs
            if filter:
                idxs = filter.satisfies_df(records, transformed) & start_idxs
            else:
                idxs = start_idxs

            if rule.type == "assign":
                values = get_value(rule.sourceField, idxs)
                set_value(rule.destinationField, values, idxs)

            elif rule.type == "find-replace":
                values = get_value(f"destination!{rule.destinationField}", idxs)
                for pair in rule.values:
                    values = values.str.replace(pair.find, pair.replace)
                set_value(rule.destinationField, values, idxs)

            elif rule.type == "ignore":
                pass

            elif rule.type == "constant":
                set_value(rule.destinationField, rule.value, idxs)

            elif rule.type == "concatenate":
                separator = rule.separator or ","
                values = as_string(get_value(rule.sourceFields[0], idxs))
                for source_field in rule.sourceFields[1:]:
                    values = (
                        values + separator + as_string(get_value(source_field, idxs))
                    )
                set_value(rule.destinationField, values, idxs)

            elif rule.type == "array":
                serieses = [get_value(f, idxs) for f in rule.sourceFields]
                arrayed = pd.Series([list(s) for s in zip(*serieses)])
                set_value(rule.destinationField, arrayed, idxs)

            elif rule.type == "transform":
                if rule.transform == "lowercase":
                    set_value(
                        rule.destinationField,
                        get_value(rule.sourceField, idxs).str.lower(),
                        idxs,
                    )
                elif rule.transform == "uppercase":
                    set_value(
                        rule.destinationField,
                        get_value(rule.sourceField, idxs).str.upper(),
                        idxs,
                    )
                else:
                    raise Exception("Unknown transform " + rule.transform)

            elif rule.type == "regex-extract":
                match_value = get_value(rule.sourceField, idxs)

                match = match_value.str.extract(rule.regex)

                match.columns = pd.Index(rule.destinationFields)

                for destination_field in rule.destinationFields:
                    set_value(
                        destination_field,
                        match[destination_field].replace(np.nan, None),
                        idxs,
                    )

            elif rule.type == "interpolate":

                def interpolate_string(idx: int) -> str:
                    values = []
                    for f in rule.sourceFields:
                        if f.startswith("destination!"):
                            values.append(
                                transformed.loc[idx].get(f.replace("destination!", ""))
                            )
                        else:
                            values.append(records.loc[idx].get(f, ""))

                    return rule.output.format(*values)

                if idxs is not None:
                    output_idx = pd.Series(records.index).loc[idxs]
                    # output = records.loc[idxs].apply(interpolate_string, axis=1)
                else:
                    output_idx = pd.Series(records.index)
                    # output = records.apply(interpolate_string, axis=1)

                output = output_idx.apply(interpolate_string)

                set_value(
                    rule.destinationField,
                    output,
                    idxs,
                )

            elif rule.type == "arithmetic":
                n = len(records)

                expr = parse_expr(rule.equation)
                fields = expr.free_symbols

                # This is inefficient, we do arithmetic on the whole dataframe
                # but then only set the values on the idxs that we care about.
                # TODO: make this operate only on the filtered rows
                res = pd.Series(
                    [parse_expr(rule.equation)] * len(records), index=records.index
                )

                for field in fields:
                    values = get_value(field.name, idxs)
                    for i in range(n):
                        res[i] = res[i].subs(field, values[i])

                set_value(
                    rule.destinationField,
                    res.apply(float).loc[idxs],
                    idxs,
                )

            elif rule.type == "delete":
                # TODO: I don't like how this is treated. Should we "delete" by
                # setting values to None? And then remove columns at the end that
                # are all None?
                if rule.destinationField.startswith("!source"):
                    raise Exception("Can't delete fields from source dataframe")

                del transformed[rule.destinationField]

            elif rule.type == "subprogram":
                subprogram = MappingProgram(rule.rules)

                subprogram.run_df(records, transformed, idxs)

            elif rule.type == "coalesce":
                # TODO: I don't think this coalesces empty strings. Should it?
                coalesced = get_value(rule.sourceFields[0], idxs)

                for source_field in rule.sourceFields[1:]:
                    coalesced = coalesced.combine_first(
                        get_value(source_field, idxs).replace(np.nan, rule.defaultValue)
                    )

                set_value(rule.destinationField, coalesced, idxs)

            elif rule.type == "nest":
                # TODO: is there a more pandas-y way of doing this?

                # Filter the dataframe down to idxs and
                # convert it to a list of records
                raw_records: List[dict] = records.loc[idxs].to_dict(orient="records")

                # Run "nest" on them
                nest_program = MappingProgram([rule])
                nested_records = nest_program.run(raw_records)

                # Each nested record looks like
                # { destinationField: [ { ... }, { ... } ]}
                # But we only want the subrecords
                subrecords = [n[rule.destinationField] for n in nested_records]

                # Now make that into a series, where the index is the filtered
                # index of the records dataframe
                nested = pd.Series(subrecords, index=records.loc[idxs].index)

                # And assign it to the destination field
                set_value(
                    rule.destinationField,
                    nested,
                    idxs,
                )

        return transformed

    def run_one(self, record: Row, initial_output: Optional[Row] = None) -> Row:
        # If output is None then create a new record.
        output: Row = initial_output or {}

        # By convention, a fieldName starting with 'destination!' refers to the
        # destination record and can use the outputs of previous rules. Otherwise
        # fieldNames refer to the source record.
        # This helper function encodes that logic.
        def get_value(field_name: str) -> Value | Row | List[Row]:
            if field_name.startswith("destination!"):
                field_name = field_name.replace("destination!", "")
                return output.get(field_name)
            else:
                return record.get(field_name)

        # Similarly, a fieldName starting with 'source!' refers to the source record.
        # I would rather that you not use that.
        def set_value(field_name: str, value: Value | Row | List[Row]):
            if field_name.startswith("source!"):
                field_name = field_name.replace("source!", "")
                record[field_name] = value
            else:
                output[field_name] = value

        for idx, rule in enumerate(self.rules):
            # If there's FFQL for this rule, we need to check that
            # this record satisfies it.
            # TODO(implement this)
            filter = self.filters[idx]
            if filter and not filter.satisfies(record, output):
                continue

            if rule.type == "assign":
                assert isinstance(rule, Assign)
                set_value(rule.destinationField, get_value(rule.sourceField))

            elif rule.type == "find-replace":
                assert isinstance(rule, FindReplace)
                value = get_value(f"destination!{rule.destinationField}")
                for pair in rule.values:
                    if value == pair.find:
                        set_value(rule.destinationField, pair.replace)
                        break

            elif rule.type == "ignore":
                # don't do anything
                assert isinstance(rule, Ignore)
            elif rule.type == "constant":
                assert isinstance(rule, Constant)
                set_value(rule.destinationField, rule.value)
            elif rule.type == "transform":
                assert isinstance(rule, Transform)
                transform_value = get_value(rule.sourceField)
                if rule.transform == "lowercase" and transform_value:
                    transform_value = str(transform_value).lower()
                elif rule.transform == "uppercase" and transform_value:
                    transform_value = str(transform_value).upper()
                set_value(rule.destinationField, transform_value)
            elif rule.type == "regex-extract":
                match_value = get_value(rule.sourceField)
                match = re.search(rule.regex, str(match_value))

                destination_fields = rule.destinationFields

                if not match:
                    # no match, so assign null to all destination fields
                    for destination_field in destination_fields:
                        set_value(destination_field, None)
                else:
                    groups = match.groups()

                    for idx, destination_field in enumerate(destination_fields):
                        if idx < len(groups):
                            set_value(destination_field, groups[idx])
                        else:
                            set_value(destination_field, None)
            elif rule.type == "interpolate":
                output_value = rule.output
                for idx, source_field in enumerate(rule.sourceFields):
                    output_value = output_value.replace(
                        "{" + str(idx) + "}", str(get_value(source_field) or "")
                    )
                set_value(rule.destinationField, output_value)
            elif rule.type == "arithmetic":
                expr = parse_expr(rule.equation)
                for field in expr.free_symbols:
                    expr = expr.subs(field, get_value(field.name))
                set_value(rule.destinationField, float(expr))
            elif rule.type == "delete":
                del output[rule.destinationField]
            elif rule.type == "subprogram":
                subprogram = MappingProgram(rule.rules)
                # Run the subprogram starting from the current output record
                output = subprogram.run_one(record, output)
            elif rule.type == "coalesce":
                found = False
                for source_field in rule.sourceFields:
                    value = get_value(source_field)
                    if value is not None:
                        set_value(rule.destinationField, value)
                        found = True
                        break
                if not found and rule.defaultValue is not None:
                    set_value(rule.destinationField, rule.defaultValue)
            elif rule.type == "concatenate":
                separator = rule.separator or ","
                values = [str(get_value(rule.sourceFields[0]) or "")]
                for source_field in rule.sourceFields[1:]:
                    values.append(separator)
                    values.append(str(get_value(source_field) or ""))
                set_value(rule.destinationField, "".join(values))

            elif rule.type == "array":
                array_values = [
                    # TODO: figure out how to get rid of this cast
                    cast(Value, get_value(source_field))
                    for source_field in rule.sourceFields
                ]
                set_value(rule.destinationField, array_values)

            elif rule.type == "nest":
                nested: Dict[str, Row] = {}

                # Each subfield contains a regex
                for nest_subfield in rule.subfields:
                    for key in record:
                        match = re.search(nest_subfield.sourceRegex, key)
                        if match:
                            # get the control
                            control = match.groups()[0]

                            # create the nested record if it doesn't exist
                            if control not in nested:
                                nested[control] = {"__control": control}

                            # and assign to it
                            nested[control][
                                nest_subfield.destinationSubfield
                            ] = get_value(key)

                # Now sort by the control field, converting to numbers if possible
                nested_records = sorted(nested.values(), key=by_control)

                # And assign to the destination field
                set_value(rule.destinationField, nested_records)

        return output

    def source_fields(self) -> List[str]:
        # Return all the source fields that this mapping program might want to use
        fields = set()

        for rule in self.rules:
            # nb: I would use "in" here but that confuses mypy
            if (
                rule.type == "assign"
                or rule.type == "ignore"
                or rule.type == "transform"
                or rule.type == "regex-extract"
            ):
                fields.add(rule.sourceField)
            elif rule.type == "arithmetic":
                expr = parse_expr(rule.equation)
                fields.update(s.name for s in expr.free_symbols)
            elif (
                rule.type == "interpolate"
                or rule.type == "coalesce"
                or rule.type == "concatenate"
                or rule.type == "array"
            ):
                fields.update(rule.sourceFields)
            elif rule.type == "subprogram":
                # Recursively get the source fields from the subprogram
                subprogram = MappingProgram(rule.rules)
                for field in subprogram.source_fields():
                    fields.add(field)
            elif (
                rule.type == "delete"
                or rule.type == "constant"
                or rule.type == "find-replace"
            ):
                # None of these require source fields
                pass
            elif rule.type == "nest":
                # Unable to get source fields from nested, because it's a regex
                pass
            else:
                raise Exception("Unknown rule type " + rule.type)

        return [f for f in fields if not f.startswith("destination!")]
