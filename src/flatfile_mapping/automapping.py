from typing import List, Optional
import os

import requests

from flatfile_mapping.mapping_rule import MappingRule, parse

# TODO: how to specify this
API_BASE_URL = "http://localhost:3000/v1"


def get_mapping_rules(
    source_fields: List[str],
    destination_fields: List[str],
    api_key: Optional[str] = None,
    base_url: str = API_BASE_URL,
    mapping_confidence_threshold: float = 0.5,
) -> List[MappingRule]:
    """
    Automap source fields to destination fields.

    Args:
        source_fields (List[str]): List of source fields
        destination_fields (List[str]): List of destination fields

    Returns:
        List[MappingRule]: List of MappingRule objects
    """
    # If no API key provided, check environment variables
    api_key = api_key or os.environ.get("FLATFILE_API_KEY")
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
                "mappingConfidenceThreshold": mapping_confidence_threshold,
            }
        ],
    }

    json = {"source": source, "destination": destination}

    response = requests.post(
        f"{base_url}/mapping",
        json=json,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    raw_rules = response.json()["data"]

    # Get rid of id and created by, and skip omit rule
    return [
        parse({k: v for k, v in raw_rule.items() if k not in ["id", "createdBy"]})
        for raw_rule in raw_rules
    ]


def get_field_weights(
    source_fields: List[str],
    destination_fields: List[str],
    api_key: Optional[str] = None,
    base_url: str = API_BASE_URL,
) -> List[MappingRule]:
    """ """
    api_key = api_key or os.environ.get("FLATFILE_API_KEY")
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
    api_key: Optional[str] = None,
    base_url: str = API_BASE_URL,
) -> List[MappingRule]:
    """ """
    api_key = api_key or os.environ.get("FLATFILE_API_KEY")
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
