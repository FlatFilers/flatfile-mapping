import os

import pytest
import requests_mock

from flatfile_mapping.automapping import automap, API_BASE_URL
from flatfile_mapping.mapping_rule import Assign

mock_response = {
    "data": [
        {
            "id": "dev_mp_yIOV5UTLm1PQMnBQ",
            "name": 'Alias "a" as "a"',
            "sourceField": "a",
            "destinationField": "a",
            "createdBy": "dev_usr_6sXgdlbF",
            "type": "assign",
        },
        {
            "id": "dev_mp_gW7zKEDOk8zBEJkO",
            "name": 'Alias "b" as "b"',
            "sourceField": "b",
            "destinationField": "b",
            "createdBy": "dev_usr_6sXgdlbF",
            "type": "assign",
        },
        {
            "id": "dev_mp_WSzOawy4jF1mtTpM",
            "name": 'Alias "c" as "c"',
            "sourceField": "c",
            "destinationField": "c",
            "createdBy": "dev_usr_6sXgdlbF",
            "type": "assign",
        },
    ]
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


@pytest.mark.skip()
def test_hit_the_api():
    source_fields = ["a", "b", "c"]
    destination_fields = ["c", "b", "a"]

    os.environ["FLATFILE_API_KEY"] = "..."
    rules = automap(source_fields, destination_fields)

    assert len(rules) == 3
    assert rules == mock_rules


def test_no_api_key():
    if "FLATFILE_API_KEY" in os.environ:
        del os.environ["FLATFILE_API_KEY"]

    source_fields = ["a", "b", "c"]
    destination_fields = ["c", "b", "a"]

    with pytest.raises(RuntimeError, match="FLATFILE_API_KEY"):
        automap(source_fields, destination_fields)


def test_hits_the_mocked_api():
    source_fields = ["a", "b", "c"]
    destination_fields = ["c", "b", "a"]

    os.environ["FLATFILE_API_KEY"] = "fake-key"

    with requests_mock.Mocker() as mock:
        mock_url = f"{API_BASE_URL}/mapping"
        mock.post(mock_url, json=mock_response)
        rules = automap(source_fields, destination_fields)

    assert len(rules) == 3
    assert rules == mock_rules
