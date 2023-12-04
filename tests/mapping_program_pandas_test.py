from copy import deepcopy
from typing import List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest


from flatfile_mapping.mapping_program import MappingProgram


def expect_source_fields(program: MappingProgram, expected: List[str]):
    assert sorted(program.source_fields()) == sorted(expected)


def assert_equal(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
    assert_frame_equal(df1, df2, check_dtype=False, check_like=True)


records = pd.DataFrame(
    [
        {"name": "Dave", "age": 42, "location": "San Francisco"},
        {"name": "Bob", "age": 32, "location": "San Francisco"},
        {"name": "Alice", "age": 22, "location": "New York"},
    ]
)


def test_simple_assign_program():
    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "nickname",
            },
            {
                "type": "assign",
                "sourceField": "age",
                "destinationField": "yearsOld",
            },
        ]
    )

    expect_source_fields(program, ["age", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nickname": "Dave",
                    "yearsOld": 42,
                },
                {
                    "nickname": "Bob",
                    "yearsOld": 32,
                },
                {
                    "nickname": "Alice",
                    "yearsOld": 22,
                },
            ]
        ),
    )


def test_simple_assign_program_with_find_replace():
    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "nickname",
            },
            {
                "type": "assign",
                "sourceField": "age",
                "destinationField": "yearsOld",
            },
            {
                "type": "find-replace",
                "destinationField": "nickname",
                "values": [
                    {"find": "Dave", "replace": "David"},
                    {"find": "Alice", "replace": "Alicia"},
                ],
            },
        ]
    )

    expect_source_fields(program, ["age", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nickname": "David",
                    "yearsOld": 42,
                },
                {
                    "nickname": "Bob",
                    "yearsOld": 32,
                },
                {
                    "nickname": "Alicia",
                    "yearsOld": 22,
                },
            ]
        ),
    )


def test_concatenate():
    program = MappingProgram.from_json(
        [
            {
                "type": "concatenate",
                "sourceFields": ["name", "age"],
                "destinationField": "nameAndAge",
            },
            {
                "type": "concatenate",
                "sourceFields": ["name", "location"],
                "destinationField": "nameAndLocation",
                "separator": " lives in ",
            },
            {
                "type": "concatenate",
                "sourceFields": ["name", "nickname", "age"],
                "destinationField": "nameAndNicknameAndAge",
            },
        ]
    )

    expect_source_fields(program, ["age", "name", "nickname", "location"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nameAndAge": "Dave,42",
                    "nameAndLocation": "Dave lives in San Francisco",
                    "nameAndNicknameAndAge": "Dave,,42",
                },
                {
                    "nameAndAge": "Bob,32",
                    "nameAndLocation": "Bob lives in San Francisco",
                    "nameAndNicknameAndAge": "Bob,,32",
                },
                {
                    "nameAndAge": "Alice,22",
                    "nameAndLocation": "Alice lives in New York",
                    "nameAndNicknameAndAge": "Alice,,22",
                },
            ]
        ),
    )


def test_array():
    program = MappingProgram.from_json(
        [
            {
                "type": "array",
                "sourceFields": ["name", "age"],
                "destinationField": "nameAndAge",
            },
            {
                "type": "array",
                "sourceFields": ["name", "nickname", "location"],
                "destinationField": "nameAndLocation",
            },
        ]
    )

    expect_source_fields(program, ["age", "name", "nickname", "location"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nameAndAge": ["Dave", 42],
                    "nameAndLocation": ["Dave", None, "San Francisco"],
                },
                {
                    "nameAndAge": ["Bob", 32],
                    "nameAndLocation": ["Bob", None, "San Francisco"],
                },
                {
                    "nameAndAge": ["Alice", 22],
                    "nameAndLocation": ["Alice", None, "New York"],
                },
            ]
        ),
    )


def test_respects_ffql_filters():
    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "nickname",
                "filter": 'location like "%New%"',
            },
            {
                "type": "assign",
                "sourceField": "age",
                "destinationField": "yearsOld",
            },
        ]
    )

    expect_source_fields(program, ["age", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    # no nickname filtered out
                    "yearsOld": 42,
                },
                {
                    # no nickname filtered out
                    "yearsOld": 32,
                },
                {
                    "yearsOld": 22,
                    "nickname": "Alice",
                },
            ],
            columns=["nickname", "yearsOld"],
        ),
    )


def test_respects_ffql_filters_on_destination_fields():
    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "nickname",
            },
            {
                "type": "transform",
                "sourceField": "destination!nickname",
                "destinationField": "nickname",
                "transform": "lowercase",
                "filter": 'destination!nickname ilike "%a%"',
            },
        ]
    )

    expect_source_fields(program, ["name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    # Lowercased b/c contains an a
                    "nickname": "dave",
                },
                {
                    # not lowercased
                    "nickname": "Bob",
                },
                {
                    # Lowercased b/c contains an a
                    "nickname": "alice",
                },
            ]
        ),
    )


def test_simple_transform_program():
    program = MappingProgram.from_json(
        [
            {
                "type": "transform",
                "sourceField": "name",
                "destinationField": "nickname",
                "transform": "uppercase",
            },
            {
                "type": "assign",
                "sourceField": "age",
                "destinationField": "yearsOld",
            },
        ]
    )

    expect_source_fields(program, ["name", "age"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nickname": "DAVE",
                    "yearsOld": 42,
                },
                {
                    "nickname": "BOB",
                    "yearsOld": 32,
                },
                {
                    "nickname": "ALICE",
                    "yearsOld": 22,
                },
            ]
        ),
    )


def test_transform_to_source_fields():
    cloned_records = deepcopy(records)

    program = MappingProgram.from_json(
        [
            {
                "type": "transform",
                "sourceField": "name",
                "destinationField": "source!name",
                "transform": "uppercase",
            }
        ]
    )

    expect_source_fields(program, ["name"])

    results = program.run_df(cloned_records)

    # results should be empty
    assert len(results.columns) == 0

    assert_equal(
        cloned_records,
        pd.DataFrame(
            [
                {"name": "DAVE", "age": 42, "location": "San Francisco"},
                {"name": "BOB", "age": 32, "location": "San Francisco"},
                {"name": "ALICE", "age": 22, "location": "New York"},
            ]
        ),
    )


def test_transform_on_destination_fields():
    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "nickname",
            },
            {
                "type": "transform",
                "sourceField": "destination!nickname",
                "destinationField": "nickname",
                "transform": "uppercase",
            },
            {
                "type": "assign",
                "sourceField": "age",
                "destinationField": "yearsOld",
            },
        ]
    )

    expect_source_fields(program, ["age", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nickname": "DAVE",
                    "yearsOld": 42,
                },
                {
                    "nickname": "BOB",
                    "yearsOld": 32,
                },
                {
                    "nickname": "ALICE",
                    "yearsOld": 22,
                },
            ]
        ),
    )


def test_field_names_can_have_spaces_in_them():
    records = pd.DataFrame(
        [
            {"first name": "Dave", "age": 42, "location": "San Francisco"},
            {"first name": "Bob", "age": 32, "location": "San Francisco"},
            {"first name": "Alice", "age": 22, "location": "New York"},
        ]
    )

    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "first name",
                "destinationField": "nick name",
            },
            {
                "type": "transform",
                "sourceField": "destination!nick name",
                "destinationField": "nick name",
                "transform": "uppercase",
            },
            {
                "type": "assign",
                "sourceField": "age",
                "destinationField": "years old",
            },
        ]
    )

    expect_source_fields(program, ["age", "first name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nick name": "DAVE",
                    "years old": 42,
                },
                {
                    "nick name": "BOB",
                    "years old": 32,
                },
                {
                    "nick name": "ALICE",
                    "years old": 22,
                },
            ]
        ),
    )


def test_coalesce():
    records = pd.DataFrame(
        [
            {"name1": "a", "name2": "b", "name3": "c"},
            {"name1": None, "name3": "f"},
            {"name2": "g", "name3": "h"},
        ]
    )

    program = MappingProgram.from_json(
        [
            {
                "type": "coalesce",
                "sourceFields": ["name1", "name2", "name3"],
                "destinationField": "name",
            },
            {
                "type": "coalesce",
                "sourceFields": ["name1", "name2"],
                "destinationField": "otherName",
                "defaultValue": "noname",
            },
        ]
    )

    expect_source_fields(program, ["name1", "name2", "name3"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "name": "a",
                    "otherName": "a",
                },
                {
                    "name": "f",
                    "otherName": "noname",
                },
                {
                    "name": "g",
                    "otherName": "g",
                },
            ]
        ),
    )


def test_regex_extract_with_single_destination_field():
    regex = "^San (.*)$"
    program = MappingProgram.from_json(
        [
            {
                "type": "regex-extract",
                "sourceField": "location",
                "destinationFields": ["saint"],
                "regex": regex,
            },
            # in the pandas case we don't want to look up fields that don't exist
            # this is a discrepancy
            # {
            #     "type": "regex-extract",
            #     "sourceField": "doesNotExist",
            #     "destinationFields": ["missing"],
            #     "regex": regex,
            # },
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "nickname",
            },
        ]
    )

    expect_source_fields(program, ["location", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "saint": "Francisco",
                    "nickname": "Dave",
                },
                {
                    "saint": "Francisco",
                    "nickname": "Bob",
                },
                {
                    "saint": None,
                    "nickname": "Alice",
                },
            ]
        ),
    )


def test_regex_extract_with_multiple_destination_fields():
    program = MappingProgram.from_json(
        [
            {
                "type": "regex-extract",
                "sourceField": "location",
                "destinationFields": ["city1", "city2"],
                "regex": "^(San) (.*)$",
            }
        ]
    )

    expect_source_fields(program, ["location"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "city1": "San",
                    "city2": "Francisco",
                },
                {
                    "city1": "San",
                    "city2": "Francisco",
                },
                {
                    "city1": None,
                    "city2": None,
                },
            ]
        ),
    )


def test_interpolate():
    program = MappingProgram.from_json(
        [
            {
                "type": "interpolate",
                "sourceFields": ["name", "age"],
                "destinationField": "greeting",
                "output": "Hello, {0}! You are {1} years old.",
            }
        ]
    )

    expect_source_fields(program, ["age", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "greeting": "Hello, Dave! You are 42 years old.",
                },
                {
                    "greeting": "Hello, Bob! You are 32 years old.",
                },
                {
                    "greeting": "Hello, Alice! You are 22 years old.",
                },
            ]
        ),
    )


def test_interpolate_with_missing_fields():
    program = MappingProgram.from_json(
        [
            {
                "type": "interpolate",
                "sourceFields": ["name", "crimes"],
                "destinationField": "greeting",
                "output": "Hello, {0}! Your crimes are: {1}.",
            }
        ]
    )

    expect_source_fields(program, ["crimes", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "greeting": "Hello, Dave! Your crimes are: .",
                },
                {
                    "greeting": "Hello, Bob! Your crimes are: .",
                },
                {
                    "greeting": "Hello, Alice! Your crimes are: .",
                },
            ]
        ),
    )


def test_arithmetic():
    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "nickname",
            },
            {
                "type": "arithmetic",
                "equation": "(age * 4) + 10",
                "sourceFields": ["age"],
                "destinationField": "iq",
            },
        ]
    )

    expect_source_fields(program, ["age", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "nickname": "Dave",
                    "iq": 178.0,
                },
                {
                    "nickname": "Bob",
                    "iq": 138.0,
                },
                {
                    "nickname": "Alice",
                    "iq": 98.0,
                },
            ]
        ),
    )


# not sure what to do here
@pytest.mark.skip
def test_arithmetic_with_bad_equation():
    program = MappingProgram.from_json(
        [
            {
                "type": "constant",
                "destinationField": "iq",
                "value": 100,
            },
            {
                "type": "arithmetic",
                # field agent doesn't exist, so this will set the value to null
                "equation": "agent * 4 + 10",
                "sourceFields": ["agent"],
                "destinationField": "iq",
            },
        ]
    )

    expect_source_fields(program, ["agent"])

    results = program.run_df(records)

    assert results == [
        {
            "iq": 100,
        },
        {
            "iq": 100,
        },
        {
            "iq": 100,
        },
    ]


def test_delete_destination_fields():
    program = MappingProgram.from_json(
        [
            {
                "type": "interpolate",
                "sourceFields": ["name", "age"],
                "destinationField": "greeting",
                "output": "Hello, {0}! You are {1} years old.",
            },
            {
                "type": "regex-extract",
                "sourceField": "destination!greeting",
                "destinationFields": ["agePart"],
                "regex": "(You are .* years old.)",
            },
            {
                "type": "delete",
                "destinationField": "greeting",
                # TODO: filter
            },
        ]
    )

    expect_source_fields(program, ["age", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "agePart": "You are 42 years old.",
                },
                {
                    "agePart": "You are 32 years old.",
                },
                {
                    "agePart": "You are 22 years old.",
                },
            ]
        ),
    )


def test_subprograms():
    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "location",
                "destinationField": "city",
            },
            {
                "name": "subprogram for young people",
                "description": "For people under 40 we give them a greeting",
                "type": "subprogram",
                "filter": "age lt 40",
                "rules": [
                    {
                        "type": "interpolate",
                        "sourceFields": ["name", "destination!city"],
                        "destinationField": "greeting",
                        "output": "Hello, young {0} in {1}!",
                    }
                ],
            },
            {
                "name": "subprogram for old people",
                "description": "For people over 40 we hide their location",
                "type": "subprogram",
                "filter": "age gte 40",
                "rules": [
                    {
                        "type": "interpolate",
                        "sourceFields": ["name"],
                        "destinationField": "greeting",
                        "output": "Hello, old {0}!",
                    },
                    {
                        "type": "constant",
                        "destinationField": "city",
                        "value": "redacted",
                    },
                ],
            },
        ]
    )

    expect_source_fields(program, ["location", "name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "city": "redacted",
                    "greeting": "Hello, old Dave!",
                },
                {
                    "city": "San Francisco",
                    "greeting": "Hello, young Bob in San Francisco!",
                },
                {
                    "city": "New York",
                    "greeting": "Hello, young Alice in New York!",
                },
            ]
        ),
    )


def test_simple_nesting():
    records = pd.DataFrame(
        [
            {"name": "Dave", "address1": "123 Main St", "address2": "Apt 1"},
            {"name": "Bob", "address1": "456 Main St", "address2": "Apt 2"},
            {"name": "Alice", "address1": "789 Main St", "address2": "Apt 3"},
        ]
    )

    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "name",
            },
            {
                "type": "nest",
                "subfields": [
                    {
                        "sourceRegex": "address([0-9]+)",
                        "destinationSubfield": "value",
                    },
                ],
                "destinationField": "address",
            },
        ]
    )

    # The nesting rule doesn't have a list of source fields.
    # This is not ideal, but what can you do?
    expect_source_fields(program, ["name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "name": "Dave",
                    "address": [
                        {"value": "123 Main St", "__control": "1"},
                        {"value": "Apt 1", "__control": "2"},
                    ],
                },
                {
                    "name": "Bob",
                    "address": [
                        {"value": "456 Main St", "__control": "1"},
                        {"value": "Apt 2", "__control": "2"},
                    ],
                },
                {
                    "name": "Alice",
                    "address": [
                        {"value": "789 Main St", "__control": "1"},
                        {"value": "Apt 3", "__control": "2"},
                    ],
                },
            ]
        ),
    )


def test_complex_nesting_rules():
    records = pd.DataFrame(
        [
            {
                "name": "Dave",
                "math.score.1": 10,
                "math.score.2": 20,
                "english.score.1": 30,
                "english.score.2": 40,
            },
            {
                "name": "Bob",
                "math.score.1": 50,
                "math.score.2": 60,
                "english.score.1": 70,
                "english.score.2": 80,
            },
        ]
    )

    program = MappingProgram.from_json(
        [
            {
                "type": "assign",
                "sourceField": "name",
                "destinationField": "name",
            },
            {
                "type": "nest",
                "subfields": [
                    {
                        "sourceRegex": "math.score.([0-9]+)",
                        "destinationSubfield": "math",
                    },
                    {
                        "sourceRegex": "english.score.([0-9]+)",
                        "destinationSubfield": "english",
                    },
                ],
                "destinationField": "scores",
            },
        ]
    )

    # The nesting rule doesn't have a list of source fields.
    # This is not ideal, but what can you do?
    expect_source_fields(program, ["name"])

    results = program.run_df(records)

    assert_equal(
        results,
        pd.DataFrame(
            [
                {
                    "name": "Dave",
                    "scores": [
                        {"math": 10, "english": 30, "__control": "1"},
                        {"math": 20, "english": 40, "__control": "2"},
                    ],
                },
                {
                    "name": "Bob",
                    "scores": [
                        {"math": 50, "english": 70, "__control": "1"},
                        {"math": 60, "english": 80, "__control": "2"},
                    ],
                },
            ]
        ),
    )
