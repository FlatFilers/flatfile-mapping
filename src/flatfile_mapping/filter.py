"""
This file contains the implementation of filtering logic.
You should not need to use anything here directly.
"""


from dataclasses import dataclass
from typing import List, Union, Any
import re

from lark import Lark, ParseTree, lexer, Tree, Token
import pandas as pd

from flatfile_mapping.mapping_rule import Row

__all__ = []


def as_number(s: Any) -> Union[int, float, None]:
    if isinstance(s, (int, float)):
        return s

    try:
        return int(s)
    except (ValueError, TypeError):
        try:
            return float(s)
        except (ValueError, TypeError):
            return None


grammar_definition = """
    start: expression

    expression: chained_expression
              | non_chained_expression

    chained_expression: non_chained_expression (operator_logical non_chained_expression)+

    non_chained_expression: logical_expression
                          | wrapped
                          | sequence

    logical_expression: term operator_logical term

    wrapped: "(" expression ")"

    sequence_term_operator_term: term operator_data term
    sequence_operator_term: operator_data term
    sequence_term_operator_status: term operator_message status
    sequence_operator_status: operator_message status
    sequence: sequence_term_operator_term
            | sequence_operator_term
            | sequence_term_operator_status
            | sequence_operator_status

    term: quoted_term
        | quoted_single_term
        | characters

    quoted_term: QUOTE string_content QUOTE | QUOTE QUOTE

    quoted_single_term: QUOTE_SINGLE string_content QUOTE_SINGLE 
                      | QUOTE_SINGLE QUOTE_SINGLE

    operator: operator_data
            | operator_logical
            | operator_message

    operator_eq: "eq"
    operator_ne: "ne"
    operator_gte: "gte"
    operator_gt: "gt"
    operator_lte: "lte"
    operator_lt: "lt"
    operator_like: "like"
    operator_ilike: "ilike"
    operator_data: operator_eq
                  | operator_ne
                  | operator_gte
                  | operator_gt
                  | operator_lte
                  | operator_lt
                  | operator_like
                  | operator_ilike

    operator_and: "and"
    operator_or: "or"
    operator_logical: operator_and | operator_or

    operator_is: "is"
    operator_message: operator_is

    status: simple_status
          | quoted_status
          | quoted_single_status

    simple_status_error: "error"
    simple_status_valid: "valid"
    simple_status: simple_status_error | simple_status_valid

    quoted_status: QUOTE simple_status QUOTE

    quoted_single_status: QUOTE_SINGLE simple_status QUOTE_SINGLE

    QUOTE: "\\""
    QUOTE_SINGLE: "'"
    characters: /[A-Za-z0-9!#-&*]+/
    string_content: /[^"']+/

    %import common.WS
    %ignore WS
"""  # noqa: E501

# TODO: add support for unicode characters
# the original regex was
#    CHARACTER: /[A-Za-z0-9!#-&*-/@[_~\u00A1-\u00FF\u0100-\u017F\u0180-\u0233]/
# but that's giving an error here


@dataclass
class Node:
    value: str
    children: List["Node"]

    @property
    def term(self) -> str:
        if self.value in ("quoted_term", "quoted_single_term"):
            if len(self.children) == 3:
                return self.children[1].value
            else:
                return ""  # pragma: no cover
        elif isinstance(self.value, str):
            return self.value
        else:
            raise ValueError(f"Not a term {self.value}")  # pragma: no cover

    @staticmethod
    def from_parse_tree(parse_tree: ParseTree) -> "Node":
        if isinstance(parse_tree, lexer.Token):
            return Node(value=parse_tree.value, children=[])  # pragma: no cover

        if len(parse_tree.children) == 1:
            first_child = parse_tree.children[0]

            if isinstance(first_child, Token):
                return Node(value=first_child.value, children=[])
            elif isinstance(first_child, str):
                return Node(value=first_child, children=[])  # pragma: no cover
            elif isinstance(first_child, Tree):
                return Node.from_parse_tree(first_child)
            else:
                raise ValueError(
                    f"Unknown child type {type(first_child)}"
                )  # pragma: no cover
        elif len(parse_tree.children) == 0:
            return Node(value=parse_tree.data, children=[])
        else:
            # In this case the value is a token
            assert isinstance(parse_tree.data, lexer.Token)

            children: List[Node] = []
            for child in parse_tree.children:
                if isinstance(child, Token):
                    children.append(Node(value=child.value, children=[]))
                elif isinstance(child, str):
                    children.append(Node(value=child, children=[]))  # pragma: no cover
                elif isinstance(child, Tree):
                    children.append(Node.from_parse_tree(child))
                else:
                    raise ValueError(
                        f"Unknown child type {type(child)}"
                    )  # pragma: no cover

            return Node(
                value=parse_tree.data.value,
                children=children,
            )


parser = Lark(grammar_definition, start="start", parser="lalr")


class Filter:
    def __init__(self, root: Node) -> None:
        self._root = root

    @staticmethod
    def from_query(query: str) -> "Filter":
        parsed = parser.parse(query)
        root = Node.from_parse_tree(parsed)
        return Filter(root)

    def satisfies(self, source: Row, destination: Row) -> bool:
        record = {**source, **{f"destination!{k}": v for k, v in destination.items()}}
        return self.filter(record)

    def satisfies_df(
        self, source: pd.DataFrame, destination: pd.DataFrame
    ) -> pd.Series:
        df = pd.concat([source, destination.add_prefix("destination!")], axis=1)
        return _filter_df(df, self._root)

    def filter(self, row: Row) -> bool:
        return _filter(row, self._root)

    def filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[_filter_df(df, self._root)]


def _filter(row: Row, node: Node) -> bool:
    value = node.value
    children = node.children

    if value == "chained_expression":
        satisfied = _filter(row, children[0])
        for i in range(1, len(children), 2):
            operator = children[i].value
            next_satisfied = _filter(row, children[i + 1])

            if operator == "operator_and":
                satisfied = satisfied and next_satisfied
            elif operator == "operator_or":
                satisfied = satisfied or next_satisfied
            else:
                raise ValueError(f"Unknown operator {operator}")  # pragma: no cover
        return satisfied

    # TODO: this code seems impossible to hit, make sure that's the case
    # elif value == "logical_expression":
    #     first = _filter(row, children[0])
    #     last = _filter(row, children[2])
    #     operator = children[1].value

    #     if operator == "operator_and":
    #         return first and last
    #     elif operator == "operator_or":
    #         return first or last
    #     else:
    #         raise ValueError(f"Unknown operator {operator}")

    # elif value == "wrapped":
    #     return _filter(row, children[1])

    elif value == "sequence_term_operator_term":
        field_name = children[0].term
        operator = children[1].value
        field_value = children[2].term
        record_value = row.get(field_name)

        field_value_as_number = as_number(field_value)
        record_value_as_number = as_number(record_value)

        if field_value_as_number is not None and record_value_as_number is not None:
            if operator == "operator_eq":
                return record_value_as_number == field_value_as_number
            elif operator == "operator_ne":
                return record_value_as_number != field_value_as_number
            elif operator == "operator_gte":
                return record_value_as_number >= field_value_as_number
            elif operator == "operator_gt":
                return record_value_as_number > field_value_as_number
            elif operator == "operator_lte":
                return record_value_as_number <= field_value_as_number
            elif operator == "operator_lt":
                return record_value_as_number < field_value_as_number
            else:  # pragma: no cover
                raise ValueError("like and ilike not supported on numbers")

        if field_value is None:
            return False
        if type(field_value) != type(record_value):
            return False

        assert isinstance(field_value, str)
        assert isinstance(record_value, str)

        if operator == "operator_eq":
            return record_value == field_value
        elif operator == "operator_ne":
            return record_value != field_value
        elif operator == "operator_gte":
            return record_value >= field_value
        elif operator == "operator_gt":
            return record_value > field_value
        elif operator == "operator_lte":
            return record_value <= field_value
        elif operator == "operator_lt":
            return record_value < field_value
        # TODO: these are both wrong
        elif operator == "operator_like":
            rgx = rf"^{field_value.replace('%', '.*')}$"
            return re.match(rgx, record_value) is not None
        elif operator == "operator_ilike":
            rgx = rf"^{field_value.replace('%', '.*')}$"
            return re.match(rgx, record_value, re.IGNORECASE) is not None

    elif value in (
        "sequence_operator_term",
        "sequence_term_operator_status",
        "sequence_operator_status",
    ):  # pragma: no cover
        raise NotImplementedError()

    raise ValueError(f"Unknown value {value}")


def _filter_df(df: pd.DataFrame, node: Node) -> pd.Series:
    value = node.value
    children = node.children

    if value == "chained_expression":
        satisfied = _filter_df(df, children[0])
        for i in range(1, len(children), 2):
            operator = children[i].value
            next_satisfied = _filter_df(df, children[i + 1])

            if operator == "operator_and":
                satisfied = satisfied & next_satisfied
            elif operator == "operator_or":
                satisfied = satisfied | next_satisfied
            else:
                raise ValueError(f"Unknown operator {operator}")

        return satisfied

    # This seems unused?
    # elif value == "logical_expression":
    #     first = _filter_df(df, children[0])
    #     last = _filter_df(df, children[2])
    #     operator = children[1].value

    #     if operator == "operator_and":
    #         return first & last
    #     elif operator == "operator_or":
    #         return first | last
    #     else:
    #         raise ValueError(f"Unknown operator {operator}")

    elif value == "wrapped":
        return _filter_df(df, children[1])

    elif value == "sequence_term_operator_term":
        field_name = children[0].term
        operator = children[1].value
        field_value = children[2].term

        record_value = df[field_name]

        # convert to number if appropriate
        field_value_as_number = as_number(field_value)
        if field_value_as_number is not None and pd.api.types.is_numeric_dtype(
            record_value.dtype
        ):
            if operator == "operator_eq":
                return record_value == field_value_as_number
            elif operator == "operator_ne":
                return record_value != field_value_as_number
            elif operator == "operator_gte":
                return record_value >= field_value_as_number
            elif operator == "operator_gt":
                return record_value > field_value_as_number
            elif operator == "operator_lte":
                return record_value <= field_value_as_number
            elif operator == "operator_lt":
                return record_value < field_value_as_number
            elif operator in ("operator_like", "operator_ilike"):
                raise ValueError("like and ilike not supported on numbers")

        if operator == "operator_eq":
            return record_value == field_value
        elif operator == "operator_ne":
            return record_value != field_value
        elif operator == "operator_gte":
            return record_value >= field_value
        elif operator == "operator_gt":
            return record_value > field_value
        elif operator == "operator_lte":
            return record_value <= field_value
        elif operator == "operator_lt":
            return record_value < field_value
        elif operator == "operator_like":
            rgx = rf"^{field_value.replace('%', '.*')}$"
            return record_value.str.match(rgx)
        elif operator == "operator_ilike":
            rgx = rf"^{field_value.replace('%', '.*')}$"
            return record_value.str.match(rgx, case=False)

    elif value in (
        "sequence_operator_term",
        "sequence_term_operator_status",
        "sequence_operator_status",
    ):  # pragma: no cover
        raise NotImplementedError()

    raise ValueError(f"Unknown value {value}")
