from typing import List
import os

import requests

from flatfile_mapping.mapping_rule import MappingRule, parse

# TODO: how to specify this
API_BASE_URL = "http://localhost:3000/v1"
API_KEY = "..."


def get_mapping_rules(
    source_fields: List[str],
    destination_fields: List[str],
    api_key: str = API_KEY,
    base_url: str = API_BASE_URL,
) -> List[MappingRule]:
    """
    Automap source fields to destination fields.

    Args:
        source_fields (List[str]): List of source fields
        destination_fields (List[str]): List of destination fields

    Returns:
        List[MappingRule]: List of MappingRule objects
    """
    api_key = os.environ.get("FLATFILE_API_KEY")
    if api_key is None:
        raise RuntimeError(
            "FLATFILE_API_KEY environment variable must be set for automapping."
        )

    source = {
        "slug": "source",
        "name": "source",
        "sheets": [
            {
                "name": "source sheet",
                "fields": [{"type": "string", "key": field} for field in source_fields],
            }
        ],
    }

    destination = {
        "slug": "destination",
        "name": "destination",
        "sheets": [
            {
                "name": "destination sheet",
                "fields": [
                    {"type": "string", "key": field} for field in destination_fields
                ],
            }
        ],
    }

    response = requests.post(
        f"{base_url}/mapping",
        json={"source": source, "destination": destination},
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    raw_rules = response.json()["data"]

    print(raw_rules)

    # Get rid of id and created by, and skip omit rule
    return [
        parse({k: v for k, v in raw_rule.items() if k not in ["id", "createdBy"]})
        for raw_rule in raw_rules
    ]


def get_field_weights(
    source_fields: List[str],
    destination_fields: List[str],
    api_key: str = API_KEY,
    base_url: str = API_BASE_URL,
) -> List[MappingRule]:
    """ """
    api_key = os.environ.get("FLATFILE_API_KEY")
    if api_key is None:
        raise RuntimeError(
            "FLATFILE_API_KEY environment variable must be set for automapping."
        )

    response = requests.post(
        f"{base_url}/mapping/field-weights",
        json={"sourceFields": source_fields, "destinationFields": destination_fields},
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    weights = response.json()["data"]["weights"]

    return weights


def get_enum_weights(
    source_field: str,
    source_values: List[str],
    destination_field: str,
    destination_values: List[str],
    api_key: str = API_KEY,
    base_url: str = API_BASE_URL,
) -> List[MappingRule]:
    """ """
    api_key = os.environ.get("FLATFILE_API_KEY")
    if api_key is None:
        raise RuntimeError(
            "FLATFILE_API_KEY environment variable must be set for automapping."
        )

    response = requests.post(
        f"{base_url}/mapping/enum-weights",
        json={
            "sourceField": source_field,
            "sourceValues": source_values,
            "destinationField": destination_field,
            "destinationValues": destination_values,
        },
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    weights = response.json()["data"]["weights"]

    return weights
