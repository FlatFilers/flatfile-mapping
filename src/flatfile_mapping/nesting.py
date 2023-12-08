"""
This file contains experimental logic for programmatically generating nesting rules.
"""

from typing import Literal, List, Optional, Dict, Any, Tuple
import re
from functools import cmp_to_key
from dataclasses import dataclass

from pydantic import BaseModel

from flatfile_mapping.mapping_rule import Nest, NestSubfield, Row

Case = Literal["camel", "pascal", "delimited", "lower", "upper"]

delimiter_regex = r"([-_ \.])"
just_delimiter_regex = r"^[-_ \.]+$"


def is_delimiter(s: str) -> bool:
    return re.match(just_delimiter_regex, s) is not None


def as_number(s: Any) -> Optional[float]:
    try:
        return float(s)
    except ValueError:
        return None


def detect_case(s: str) -> Case:
    if re.match("^[a-z0-9*]+$", s):
        return "lower"
    elif re.match("^[A-Z0-9*]+$", s):
        return "upper"
    elif re.match("^[a-z0-9]+(?:[A-Z][a-z0-9]*)*$", s):
        # camelCaseLooksLikeThis
        return "camel"
    elif re.match("^[A-Z][a-z0-9]*(?:[A-Z][a-z0-9]*)*$", s):
        return "pascal"
    else:
        return "delimited"


class Splitted(BaseModel):
    original: str
    case: Case
    parts: List[str]


# if you have like "23423aa" or "aa23423" then split into two pieces
# this is necessary for field names like "address1", "address2", etc
def split_numbers(s: str) -> List[str]:
    number_first = re.match("^([0-9]+)([^0-9]+)$", s)
    number_last = re.match("^([^0-9]+)([0-9]+$)", s)

    if number_first:
        return list(number_first.groups())
    elif number_last:
        return list(number_last.groups())
    else:
        return [s]


def to_parts(s: str) -> Splitted:
    splitted = _to_parts(s)
    splitted.parts = [n for part in splitted.parts for n in split_numbers(part)]

    return splitted


def _to_parts(s: str) -> Splitted:
    splitted = Splitted(original=s, case=detect_case(s), parts=[])

    if splitted.case in ["lower", "upper"]:
        # Split into numeric and non-numeric parts, keep all of them
        splitted.parts = re.split("([0-9]+)", s)
        splitted.parts = [p for p in splitted.parts if p != ""]
    elif splitted.case == "camel":
        splitted.parts = re.split("(?=[A-Z])", s)
    elif splitted.case == "delimited":
        splitted.parts = re.split(delimiter_regex, s)
    elif splitted.case == "pascal":
        splitted.parts = re.split("(?=[A-Z])", s)

    return splitted


def rejoin(parts: List[str], case_: Case) -> str:
    # Remove delimiter from beginning and end
    if is_delimiter(parts[0]):
        parts = parts[1:]
    if is_delimiter(parts[-1]):
        parts = parts[:-1]

    # Remove consecutive delimiters
    new_parts: List[str] = []
    for part in parts:
        if is_delimiter(part) and is_delimiter(new_parts[-1]):
            pass
        else:
            new_parts.append(part)

    return "".join(new_parts)


def almost_match_at(s1: Splitted, s2: Splitted) -> Optional[int]:
    # If they have different casing, they're not a match
    if s1.case != s2.case:
        return None

    # If either has one part, they're not a match
    if len(s1.parts) == 1 or len(s2.parts) == 1:
        return None

    # If they have a different number of parts, they're not a match
    if len(s1.parts) != len(s2.parts):
        return None

    # Otherwise they're a match if they agree in all but one part
    # and we return the index of that part

    mismatch_index: Optional[int] = None

    for i, (p1, p2) in enumerate(zip(s1.parts, s2.parts)):
        if p1 != p2 and mismatch_index is not None:
            # multiple mismatches
            return None
        elif p1 != p2:
            mismatch_index = i

    return mismatch_index


class GroupedField(BaseModel):
    idx: int
    key: str
    prettyKey: str
    splitted: Splitted
    control: str
    controlIndex: int


Groups = Dict[str, List[GroupedField]]


# A group is a set of fields that have the same name except for one "part"
def find_groups(field_names: List[str]) -> Groups:
    groups: Groups = {}

    splitted = [to_parts(field_name) for field_name in field_names]

    for i, si in enumerate(splitted):
        for j, sj in enumerate(splitted):
            if i >= j:
                continue

            mismatch_index = almost_match_at(si, sj)
            if mismatch_index is not None:
                key = rejoin(
                    si.parts[:mismatch_index] + ["*"] + si.parts[mismatch_index + 1 :],
                    si.case,
                )

                prettyKey = rejoin(
                    si.parts[:mismatch_index] + si.parts[mismatch_index + 1 :], si.case
                )

                if key not in groups:
                    groups[key] = []

                seen_indexes = [g.idx for g in groups[key]]

                if i not in seen_indexes:
                    groups[key].append(
                        GroupedField(
                            idx=i,
                            key=key,
                            prettyKey=prettyKey,
                            splitted=splitted[i],
                            control=splitted[i].parts[mismatch_index],
                            controlIndex=mismatch_index,
                        )
                    )

                if j not in seen_indexes:
                    groups[key].append(
                        GroupedField(
                            idx=j,
                            key=key,
                            prettyKey=prettyKey,
                            splitted=splitted[j],
                            control=splitted[j].parts[mismatch_index],
                            controlIndex=mismatch_index,
                        )
                    )

    return groups


class GroupSummary(BaseModel):
    idxs: List[int]
    num_numbers: int


def cmp(pair1: Tuple[str, GroupSummary], pair2: Tuple[str, GroupSummary]) -> int:
    s1 = pair1[1]
    s2 = pair2[1]

    # First, sort by number of numbers
    diff_num_numbers = s2.num_numbers - s1.num_numbers
    if diff_num_numbers != 0:
        return diff_num_numbers

    # Then sort by number of indexes
    return len(s2.idxs) - len(s1.idxs)


def distill_groups(groups: Groups, keep_non_numeric: bool = False) -> Groups:
    # We want each field to appear in at most one group.
    # We will use the following heuristics:
    # 1. if there is a group where all of the controls are numbers,
    #    we prefer that one
    # 2. otherwise, we prefer groups that have more fields in them

    result: Groups = {}

    # summarize each group
    summaries = {
        key: GroupSummary(
            idxs=[g.idx for g in group],
            num_numbers=len([g for g in group if as_number(g.control) is not None]),
        )
        for key, group in groups.items()
    }

    # sort the keys from best to worst
    sorted_items = sorted(summaries.items(), key=cmp_to_key(cmp))
    sorted_keys = [key for key, _ in sorted_items]

    # Then add them to the result in that order, don't add ones that conflict
    # with already added ones
    idxs_seen = set()

    for key in sorted_keys:
        summary = summaries[key]

        # if we need to remove non-numeric then do so
        if summary.num_numbers < len(summary.idxs) and not keep_non_numeric:
            continue

        # skip if we've already seen any of these indexes
        if any(idx in idxs_seen for idx in summary.idxs):
            continue

        # otherwise add this to the result
        result[key] = groups[key]

        # and add the indexes to the set
        idxs_seen.update(summary.idxs)

    return result


NestedRow = Dict[str, Any]


def make_nesting_rules(groups: Groups) -> List[Nest]:
    rules: List[Nest] = []

    for group_key, fields in groups.items():
        # This can be any matched field, it doesn't matter
        matched_field = fields[0]
        is_numeric = all(as_number(f.control) is not None for f in fields)

        parts = (
            ["^"]
            + matched_field.splitted.parts[: matched_field.controlIndex]
            + ["([0-9]+)" if is_numeric else "(.+)"]
            + matched_field.splitted.parts[matched_field.controlIndex + 1 :]
            + ["$"]
        )

        source_regex = rejoin(parts, matched_field.splitted.case)

        rules.append(
            Nest(
                type="nest",
                subfields=[
                    NestSubfield(
                        sourceRegex=source_regex,
                        destinationSubfield=matched_field.prettyKey,
                    )
                ],
                destinationField=matched_field.prettyKey,
            )
        )

    return rules


def control_cmp(row1, row2) -> int:
    num1 = as_number(row1["__control"])
    num2 = as_number(row2["__control"])

    if num1 is not None and num2 is not None:
        return int(1000 * (num1 - num2))
    elif num1 is not None:
        return -1
    elif num2 is not None:
        return 1
    elif row1["__control"] == row2["__control"]:
        return 0
    else:
        return 1 if row1["__control"] > row2["__control"] else -1


def do_nest(groups: Groups, record: Row) -> NestedRow:
    nested_row: NestedRow = {}
    seen_fields = set()

    for group_key, fields in groups.items():
        # Get rid of the *
        key = fields[0].prettyKey

        nested_row[key] = []

        for field in fields:
            nested_row[key].append(
                {
                    key: record[field.splitted.original],
                    "__control": field.control,
                }
            )
            seen_fields.add(field.splitted.original)

        # Now sort by the control field, converting to numbers if possible
        nested_row[key].sort(key=cmp_to_key(control_cmp))

    for key, value in record.items():
        if key not in seen_fields:
            nested_row[key] = value

    return nested_row


def combine(
    nested_row: NestedRow, destination_field: str, field_names: List[str]
) -> NestedRow:
    combined: NestedRow = {destination_field: []}
    new_nests: Dict[str, dict] = {}

    for field_name in field_names:
        subarray = nested_row[field_name]
        if not isinstance(subarray, list):
            raise Exception("Expected subarray to be a list")
        for obj in subarray:
            control = obj["__control"]
            if control not in new_nests:
                new_nests[control] = {"__control": control}
            new_nests[control] = {**new_nests[control], **obj}

    combined[destination_field] = sorted(
        new_nests.values(), key=cmp_to_key(control_cmp)
    )

    for k, v in nested_row.items():
        if k not in field_names:
            combined[k] = v

    return combined


@dataclass
class Signature:
    name: str
    subfield_renames: Dict[str, str]


def make_signature(field_names: List[str]) -> Optional[Signature]:
    """
    Given a bunch of field names that we've already decided to combine,
    try to
    1. come up with a good name for the combined field, and
    2. come up with good names for the subfields
    """
    # no field names, no signature
    if len(field_names) == 0:
        return None

    # split each field name into parts
    splitteds = [to_parts(field_name) for field_name in field_names]
    splitted0 = splitteds[0]

    # find all indices where all of the field names agree
    good_indices = {i for i in range(len(splitted0.parts))}

    for splitted in splitteds[1:]:
        good_indices = {
            i
            for i in good_indices
            if i < len(splitted.parts) and splitted.parts[i] == splitted0.parts[i]
        }

    # if there are not any good indices, return None
    if not good_indices:
        return None

    # otherwise "rejoin" the common parts of the name
    # so if the fields were "address_street", "address_city", "address_zip"
    # the common part would be "address"
    signature = rejoin(
        [splitted0.parts[i] for i in sorted(good_indices)], splitted0.case
    )

    # and remove the common part for the subfield names
    # so you'd get "address_street" -> "street", and so on
    renames = {}
    for splitted in splitteds:
        new_name = rejoin(
            [part for i, part in enumerate(splitted.parts) if i not in good_indices],
            splitted.case,
        )
        renames[splitted.original] = new_name

    # the signature has to have at least one letter
    if re.match("[a-zA-Z]", signature):
        return Signature(signature, renames)

    return None


def recombine_on_controls(nested_row: NestedRow) -> NestedRow:
    """
    Given a row with some nested data, "recombine" the nested data
    by looking for nested fields that have the exact same "__control" values.
    TODO: do we need more permissive logic than this
    """
    recombined: NestedRow = {}

    fields_by_control_signature: Dict[str, List[str]] = {}
    used = set()

    for k, v in nested_row.items():
        if isinstance(v, list):
            controls = [row["__control"] for row in v if "__control" in row]
            if len(controls) < len(v):
                # not enough controls, so don't do nesting
                continue
            signature = ";".join(controls)
            if signature not in fields_by_control_signature:
                fields_by_control_signature[signature] = []
            fields_by_control_signature[signature].append(k)

    for signature, fields in fields_by_control_signature.items():
        recombined_objs = []
        if len(fields) > 1:
            # Start with just the controls
            recombined_objs = [
                {"__control": obj["__control"]} for obj in nested_row[fields[0]]
            ]

            for field in fields:
                used.add(field)
                for i, obj in enumerate(nested_row[field]):
                    recombined_objs[i] = {**recombined_objs[i], **obj}

            # TODO: what can I use that's not the signature here?
            combination_name = make_signature(fields)
            if combination_name:
                key = combination_name.name
                recombined_objs = [
                    {
                        combination_name.subfield_renames.get(k, k): v
                        for k, v in obj.items()
                    }
                    for obj in recombined_objs
                ]
            else:
                key = signature
            recombined[key] = recombined_objs

    for k, v in nested_row.items():
        if k not in used:
            recombined[k] = v

    return recombined


def yolo(records: List[Row]) -> List[Row]:
    field_names = list(records[0].keys())
    groups = find_groups(field_names)
    distilled = distill_groups(groups)
    nested = [do_nest(distilled, row) for row in records]
    combined = [recombine_on_controls(row) for row in nested]
    return combined


# # experimental / unused
# @dataclass
# class Combine:
#     destination_field_name: str
#     fields: List[str]


# def suggest_combine(nested_row: NestedRow) -> List[Combine]:
#     """suggest a combine regex"""
#     groups = {}

#     for k, v in nested_row.items():
#         if isinstance(v, list) and len(v) > 0:
#             # Find the controls
#             controls = [row["__control"] for row in v if "__control" in row]
#             if controls:
#                 group_key = ";".join(controls)
#                 if group_key not in groups:
#                     groups[group_key] = []
#                 groups[group_key].append(k)

#     output: List[Combine] = []
#     for group_key, fields in groups.items():
#         if len(fields) > 1:
#             output.append(Combine(destination_field_name=group_key, fields=fields))
