from flatfile_mapping.nesting import (
    combine,
    distill_groups,
    do_nest,
    find_groups,
    GroupedField,
    make_nesting_rules,
    to_parts,
)
from flatfile_mapping.mapping_rule import Nest, NestSubfield

from flatfile_mapping.nesting import recombine_on_controls


def test_works_with_lower_case():
    field_names = [
        "name1",
        "name2",
        "name3",
        "address1",
        "address2",
        "address3",
        "id",
    ]

    groups = find_groups(field_names)

    assert sorted(groups.keys()) == ["*1", "*2", "*3", "address*", "name*"]

    assert groups["name*"] == [
        GroupedField(
            idx=0,
            key="name*",
            prettyKey="name",
            splitted=to_parts("name1"),
            control="1",
            controlIndex=1,
        ),
        GroupedField(
            idx=1,
            key="name*",
            prettyKey="name",
            splitted=to_parts("name2"),
            control="2",
            controlIndex=1,
        ),
        GroupedField(
            idx=2,
            key="name*",
            prettyKey="name",
            splitted=to_parts("name3"),
            control="3",
            controlIndex=1,
        ),
    ]

    distilled = distill_groups(groups)

    assert sorted(distilled.keys()) == ["address*", "name*"]

    record = {
        "id": "123",
        "name1": "John",
        "name2": "Paul",
        "name3": "George",
        "address1": "123 Main St",
        "address2": "456 Main St",
        "address3": "789 Main St",
    }

    nested = do_nest(distilled, record)

    assert nested == {
        "id": "123",
        "name": [
            {
                "name": "John",
                "__control": "1",
            },
            {
                "name": "Paul",
                "__control": "2",
            },
            {
                "name": "George",
                "__control": "3",
            },
        ],
        "address": [
            {
                "address": "123 Main St",
                "__control": "1",
            },
            {
                "address": "456 Main St",
                "__control": "2",
            },
            {
                "address": "789 Main St",
                "__control": "3",
            },
        ],
    }

    rules = make_nesting_rules(distilled)

    assert rules == [
        Nest(
            type="nest",
            destinationField="name",
            subfields=[
                NestSubfield(
                    sourceRegex="^name([0-9]+)$",
                    destinationSubfield="name",
                )
            ],
        ),
        Nest(
            type="nest",
            destinationField="address",
            subfields=[
                NestSubfield(
                    sourceRegex="^address([0-9]+)$",
                    destinationSubfield="address",
                )
            ],
        ),
    ]


def test_dot_case():
    field_names = [
        "math.score.2001",
        "math.score.2002",
        "math.score.2003",
        "english.score.2001",
        "english.score.2002",
        "english.score.2003",
        "name",
    ]

    groups = find_groups(field_names)

    assert sorted(groups.keys()) == [
        "*.score.2001",
        "*.score.2002",
        "*.score.2003",
        "english.score.*",
        "math.score.*",
    ]

    distilled = distill_groups(groups)

    assert sorted(distilled.keys()) == ["english.score.*", "math.score.*"]

    record = {
        "name": "John",
        "math.score.2001": "100",
        "math.score.2002": "99",
        "math.score.2003": "97",
        "english.score.2001": "75",
        "english.score.2002": "52",
        "english.score.2003": "63",
    }

    nested = do_nest(distilled, record)

    assert nested == {
        "name": "John",
        "math.score": [
            {
                "math.score": "100",
                "__control": "2001",
            },
            {
                "math.score": "99",
                "__control": "2002",
            },
            {
                "math.score": "97",
                "__control": "2003",
            },
        ],
        "english.score": [
            {
                "english.score": "75",
                "__control": "2001",
            },
            {
                "english.score": "52",
                "__control": "2002",
            },
            {
                "english.score": "63",
                "__control": "2003",
            },
        ],
    }

    # complex nesting
    complex_nested = combine(nested, "score", ["math.score", "english.score"])

    assert complex_nested == {
        "name": "John",
        "score": [
            {
                "math.score": "100",
                "english.score": "75",
                "__control": "2001",
            },
            {
                "math.score": "99",
                "english.score": "52",
                "__control": "2002",
            },
            {
                "math.score": "97",
                "english.score": "63",
                "__control": "2003",
            },
        ],
    }

    recombined = recombine_on_controls(nested)

    assert recombined == {
        "name": "John",
        "score": [
            {
                "math": "100",
                "english": "75",
                "__control": "2001",
            },
            {
                "math": "99",
                "english": "52",
                "__control": "2002",
            },
            {
                "math": "97",
                "english": "63",
                "__control": "2003",
            },
        ],
    }

    rules = make_nesting_rules(distilled)

    assert rules == [
        Nest(
            type="nest",
            destinationField="math.score",
            subfields=[
                NestSubfield(
                    sourceRegex="^math.score.([0-9]+)$",
                    destinationSubfield="math.score",
                )
            ],
        ),
        Nest(
            type="nest",
            destinationField="english.score",
            subfields=[
                NestSubfield(
                    sourceRegex="^english.score.([0-9]+)$",
                    destinationSubfield="english.score",
                )
            ],
        ),
    ]
