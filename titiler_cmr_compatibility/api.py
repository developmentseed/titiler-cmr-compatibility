"""
CMR API interaction functions.

This module provides functions to interact with NASA's Common Metadata Repository (CMR)
API to fetch collection and granule metadata.
"""

import random
import logging
from typing import Optional, List, Dict, Any

import requests

from .constants import GRANULES_SEARCH_URL, COLLECTIONS_SEARCH_URL

logger = logging.getLogger(__name__)


def fetch_cmr_collections(
    page_size: int = 10,
    concept_id: Optional[str] = None,
    page_num: Optional[int] = None
) -> tuple[List[Dict[str, Any]], int]:
    """
    Fetch collections from CMR in UMM JSON format.

    Args:
        page_size: Number of collections to retrieve per request
        concept_id: Optional specific collection concept ID to search for
        page_num: Optional page number for pagination (1-indexed)

    Returns:
        Tuple of (list of collection metadata dictionaries, total hits count)

    Raises:
        requests.exceptions.RequestException: If the API request fails
    """
    # These params return matching collections exactly the same number as the search UI (10,752)
    params = {
        "page_size": page_size,
        "has_granules_or_cwic": "true",
        "sort_key[]": "-usage_score",
        "processing_level_id[]": ["3", "4"],
    }

    # Add concept_id parameter if provided for debugging
    if concept_id:
        params["concept_id"] = concept_id

    # Add page_num for pagination
    if page_num is not None:
        params["page_num"] = page_num

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        response = requests.get(
            COLLECTIONS_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()
        total_hits = data.get('hits', 0)
        logger.info(f"Total hits: {total_hits}")
        return data.get("items", []), total_hits
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching collections from CMR: {e}")
        raise


def fetch_random_granule_metadata(concept_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch random granule metadata for a given collection concept ID.

    This function first determines the total number of granules, then randomly
    selects one by offset to avoid bias toward the first granules.

    Args:
        concept_id: Collection concept ID

    Returns:
        Random granule metadata dictionary or None if no granules found

    Raises:
        requests.exceptions.RequestException: If the API request fails
    """
    params = {
        "collection_concept_id": concept_id,
        "page_size": 1
    }

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        # First request to get total count
        response = requests.get(
            GRANULES_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()

        total_num_granules = response.json().get("hits")

        if not total_num_granules:
            logger.warning(f"No granules found for collection {concept_id}")
            return None

        # CMR has a 1 million item pagination limit
        params["offset"] = random.randint(0, min(total_num_granules, int(1e6)))

        # Second request to fetch the random granule
        response = requests.get(
            GRANULES_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()
        granules = data.get("items", [])

        if granules:
            return granules[0]
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching granule metadata for collection {concept_id}: {e}")
        raise


def fetch_granule_by_id(granule_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch granule metadata by granule concept ID.

    Args:
        granule_id: Granule concept ID

    Returns:
        Granule metadata dictionary or None if not found

    Raises:
        requests.exceptions.RequestException: If the API request fails
    """
    params = {
        "concept_id": granule_id,
        "page_size": 1
    }

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        response = requests.get(
            GRANULES_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()
        granules = data.get("items", [])

        if granules:
            return granules[0]

        logger.warning(f"No granule found with ID {granule_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching granule metadata for granule {granule_id}: {e}")
        raise
