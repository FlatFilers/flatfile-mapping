from typing import List, Optional
import os

import requests

from flatfile_mapping.mapping_rule import MappingRule, parse

__all__ = ["get_mapping_rules"]

API_BASE_URL = os.environ.get("FLATFILE_API_BASE_URL", "https://api.x.flatfile.com/v1")


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
        "name": "source sheet",
        "fields": [{"type": "string", "key": field} for field in source_fields],
    }

    destination = {
        "name": "destination sheet",
        "fields": [{"type": "string", "key": field} for field in destination_fields],
        "mappingConfidenceThreshold": mapping_confidence_threshold,
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

    raw_rules = response.json()["data"]["rules"]

    return [parse(raw_rule["config"]) for raw_rule in raw_rules]
