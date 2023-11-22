from typing import List
import os

import requests

from flatfile_mapping.mapping_rule import MappingRule, parse

# TODO: how to specify this
API_BASE_URL = "http://localhost:3000/v1"


def automap(
    source_fields: List[str], destination_fields: List[str]
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
        f"{API_BASE_URL}/mapping",
        json={"source": source, "destination": destination},
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    raw_rules = response.json()["data"]

    # Get rid of id and created by
    return [
        parse({k: v for k, v in raw_rule.items() if k not in ["id", "createdBy"]})
        for raw_rule in raw_rules
    ]
