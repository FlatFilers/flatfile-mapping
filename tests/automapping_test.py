import os

import pytest
import requests_mock

from flatfile_mapping.automapping import (
    get_mapping_rules,
    API_BASE_URL,
)
from flatfile_mapping.mapping_rule import Assign, Ignore

HIT_THE_API = os.environ.get("HIT_THE_API", False)
FLATFILE_API_KEY = os.environ.get("FLATFILE_API_KEY", "...")

mock_response = {
    "data": {
        "programId": "blah blah",
        "rules": [
            {
                "id": "dev_mp_yIOV5UTLm1PQMnBQ",
                "name": 'Alias "a" as "a"',
                "config": {
                    "sourceField": "a",
                    "destinationField": "a",
                    "name": 'Alias "a" as "a"',
                    "type": "assign",
                },
                "createdBy": "dev_usr_6sXgdlbF",
                "type": "assign",
            },
            {
                "id": "dev_mp_gW7zKEDOk8zBEJkO",
                "name": 'Alias "b" as "b"',
                "config": {
                    "sourceField": "b",
                    "destinationField": "b",
                    "name": 'Alias "b" as "b"',
                    "type": "assign",
                },
                "createdBy": "dev_usr_6sXgdlbF",
                "type": "assign",
            },
            {
                "id": "dev_mp_WSzOawy4jF1mtTpM",
                "name": 'Alias "c" as "c"',
                "config": {
                    "sourceField": "c",
                    "destinationField": "c",
                    "name": 'Alias "c" as "c"',
                    "type": "assign",
                },
                "createdBy": "dev_usr_6sXgdlbF",
                "type": "assign",
            },
        ],
    }
}

mock_rules = [
    Assign(
        sourceField="a",
        destinationField="a",
        type="assign",
        name='Alias "a" as "a"',
    ),
    Assign(
        sourceField="b",
        destinationField="b",
        type="assign",
        name='Alias "b" as "b"',
    ),
    Assign(
        sourceField="c",
        destinationField="c",
        type="assign",
        name='Alias "c" as "c"',
    ),
]


@pytest.mark.skipif(not HIT_THE_API, reason="Don't hit the API")
def test_hit_the_api_for_rules():
    source_fields = ["a", "b", "c", "d"]
    destination_fields = ["c", "b", "a"]

    rules = get_mapping_rules(
        source_fields,
        destination_fields,
        api_key=FLATFILE_API_KEY,
    )

    assert len(rules) == 4
    assert sorted(rules, key=lambda rule: rule.name) == sorted(
        mock_rules + [Ignore(sourceField="d", type="ignore", name='Ignore "d"')],
        key=lambda rule: rule.name,
    )


def test_no_api_key():
    if "FLATFILE_API_KEY" in os.environ:
        del os.environ["FLATFILE_API_KEY"]

    source_fields = ["a", "b", "c"]
    destination_fields = ["c", "b", "a"]

    with pytest.raises(RuntimeError, match="FLATFILE_API_KEY"):
        get_mapping_rules(source_fields, destination_fields)


def test_hits_the_mocked_api():
    source_fields = ["a", "b", "c"]
    destination_fields = ["c", "b", "a"]

    os.environ["FLATFILE_API_KEY"] = "fake-key"

    with requests_mock.Mocker() as mock:
        mock_url = f"{API_BASE_URL}/mapping"
        mock.post(mock_url, json=mock_response)
        rules = get_mapping_rules(source_fields, destination_fields)

    assert len(rules) == 3
    assert rules == mock_rules
