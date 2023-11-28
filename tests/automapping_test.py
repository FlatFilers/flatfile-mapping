import os

import pytest
import requests_mock

from flatfile_mapping.automapping import (
    get_enum_weights,
    get_mapping_rules,
    get_field_weights,
    API_BASE_URL,
)
from flatfile_mapping.mapping_rule import Assign, Ignore

HIT_THE_API = os.environ.get("HIT_THE_API", False)

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


@pytest.mark.skipif(not HIT_THE_API, reason="Don't hit the API")
def test_hit_the_api():
    source_fields = ["a", "b", "c", "d"]
    destination_fields = ["c", "b", "a"]

    os.environ["FLATFILE_API_KEY"] = "..."
    rules = get_mapping_rules(source_fields, destination_fields)

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


@pytest.mark.skipif(not HIT_THE_API, reason="Don't hit the API")
def test_hit_the_api_for_field_weights():
    source_fields = ["car", "house", "airplane"]
    destination_fields = ["automobile", "home", "flyer"]

    os.environ["FLATFILE_API_KEY"] = "..."
    weights = get_field_weights(source_fields, destination_fields)

    assert len(weights) == 9

    car_auto = [
        w for w in weights if w["source"] == "car" and w["destination"] == "automobile"
    ]

    assert len(car_auto) == 1
    assert car_auto[0]["weight"] > 0


def test_hits_the_mocked_api_for_field_weights():
    source_fields = ["animal", "banana", "chair"]
    destination_fields = ["cake", "bakery", "apple"]

    mock_weights = [
        {"source": s, "destination": d, "weight": 0.5}
        for s in source_fields
        for d in destination_fields
    ]

    mock_response = {"data": {"weights": mock_weights}}

    os.environ["FLATFILE_API_KEY"] = "fake-key"

    with requests_mock.Mocker() as mock:
        mock_url = f"{API_BASE_URL}/mapping/field-weights"
        mock.post(mock_url, json=mock_response)
        weights = get_field_weights(source_fields, destination_fields)

    assert len(weights) == 9
    assert weights == mock_weights


@pytest.mark.skipif(not HIT_THE_API, reason="Don't hit the API")
def test_hit_the_api_for_enum_weights():
    source_field = "name"
    source_values = ["car", "house", "airplane"]
    destination_field = "nickname"
    destination_values = ["automobile", "home", "flyer"]

    os.environ["FLATFILE_API_KEY"] = "..."
    weights = get_enum_weights(
        source_field, source_values, destination_field, destination_values
    )

    assert len(weights) == 9

    car_auto = [
        w for w in weights if w["source"] == "car" and w["destination"] == "automobile"
    ]

    assert len(car_auto) == 1
    assert car_auto[0]["weight"] > 0


def test_hits_the_mocked_api_for_enum_weights():
    source_field = "name"
    source_values = ["car", "house", "airplane"]
    destination_field = "nickname"
    destination_values = ["automobile", "home", "flyer"]

    mock_weights = [
        {"source": s, "destination": d, "weight": 0.5}
        for s in source_values
        for d in destination_values
    ]

    mock_response = {"data": {"weights": mock_weights}}

    os.environ["FLATFILE_API_KEY"] = "fake-key"

    with requests_mock.Mocker() as mock:
        mock_url = f"{API_BASE_URL}/mapping/enum-weights"
        mock.post(mock_url, json=mock_response)
        weights = get_enum_weights(
            source_field, source_values, destination_field, destination_values
        )

    assert len(weights) == 9
    assert weights == mock_weights
