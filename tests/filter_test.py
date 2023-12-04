from typing import List

import pandas as pd
from pandas.testing import assert_frame_equal

from flatfile_mapping.mapping_rule import Row
from flatfile_mapping.filter import Filter

records: List[Row] = [
    {"name": "Dave", "age": 23},
    {"name": "John", "age": 34},
    {"name": "Jane", "age": 45},
]

destination_records: List[Row] = [{"output": 1}, {"output": 2}, {"output": 3}]

df = pd.DataFrame(records)
destination_df = pd.DataFrame(destination_records)


class TestRecords:
    def test_simple(self):
        filter = Filter.from_query("name eq John")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[1]]

    def test_binary_ops(self):
        filter = Filter.from_query("age eq 34")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[1]]

        filter = Filter.from_query("age gt 34")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[2]]

        filter = Filter.from_query("age lt 34")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[0]]

        filter = Filter.from_query("age gte 34")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[1], records[2]]

        filter = Filter.from_query("age lte 34")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[0], records[1]]

        filter = Filter.from_query("age ne 34")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[0], records[2]]

    def test_single_quoted(self):
        filter = Filter.from_query("name eq 'John'")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[1]]

    def test_double_quoted(self):
        filter = Filter.from_query('name eq "John"')
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[1]]

    def test_and(self):
        filter = Filter.from_query("(name eq John) and (age eq 34)")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[1]]

    def test_or(self):
        filter = Filter.from_query("(name eq John) or (age eq 45)")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == records[1:]

    def test_like(self):
        filter = Filter.from_query("name like J%")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == records[1:]

        filter = Filter.from_query("name like %a%")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == [records[0], records[2]]

        filter = Filter.from_query("name like j%")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == []

    def test_ilike(self):
        filter = Filter.from_query("name ilike j%")
        filtered = [row for row in records if filter.filter(row)]
        assert filtered == records[1:]

    def test_satisfies_or(self):
        filter = Filter.from_query("(name eq John) or (destination!output gt 2)")
        filtered = [
            row
            for row, output_row in zip(records, destination_records)
            if filter.satisfies(row, output_row)
        ]
        assert filtered == records[1:]

    def test_satisfies_and(self):
        filter = Filter.from_query(
            "((name eq John) and (name eq John)) and (destination!output eq 2)"
        )
        filtered = [
            row
            for row, output_row in zip(records, destination_records)
            if filter.satisfies(row, output_row)
        ]
        assert filtered == records[1:2]


class TestDataFrame:
    def test_simple(self):
        filter = Filter.from_query("name eq John")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[1:2])

    def test_single_quoted(self):
        filter = Filter.from_query("name eq 'John'")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[1:2])

    def test_double_quoted(self):
        filter = Filter.from_query('name eq "John"')
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[1:2])

    def test_and(self):
        filter = Filter.from_query("(name eq John) and (age eq 34)")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[1:2])

    def test_or(self):
        filter = Filter.from_query("(name eq John) or (age eq 45)")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[1:])

    def test_like(self):
        filter = Filter.from_query("name like J%")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[1:])

        filter = Filter.from_query("name like %a%")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[[0, 2]])

        filter = Filter.from_query("name like j%")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[[]])

    def test_ilike(self):
        filter = Filter.from_query("name ilike j%")
        filtered = filter.filter_df(df)

        assert_frame_equal(filtered, df.iloc[1:])

    def test_satisfies(self):
        filter = Filter.from_query("(name eq John) or (destination!output gt 2)")
        indexer = filter.satisfies_df(df, destination_df)
        filtered = df[indexer]

        assert_frame_equal(filtered, df.iloc[1:])
