"""
CMR API interaction functions.

This module provides functions to interact with NASA's Common Metadata Repository (CMR)
API to fetch collection and granule metadata.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Sequence
from typing import Any

from httpx2 import AsyncClient, HTTPError

from .constants import COLLECTIONS_SEARCH_URL, GRANULES_SEARCH_URL
from .get_eosdis_providers import get_eosdis_shortnames

logger = logging.getLogger(__name__)

CMR_HEADERS = {
    "Accept": "application/vnd.nasa.cmr.umm_results+json",
}
DEFAULT_TIMEOUT = 30


async def fetch_cmr_collections(
    page_size: int = 10,
    concept_id: str | None = None,
    page_num: int | None = None,
    client: AsyncClient | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> tuple[list[dict[str, Any]], int]:
    """
    Fetch collections from CMR in UMM JSON format.

    Args:
        page_size: Number of collections to retrieve per request.
        concept_id: Optional specific collection concept ID to search for.
        page_num: Optional page number for pagination (1-indexed).
        client: Optional shared HTTP client.
        timeout: Request timeout in seconds.

    Returns:
        Tuple of (list of collection metadata dictionaries, total hits count).

    Raises:
        httpx2.HTTPError: If the API request fails.
    """
    # These params return matching collections exactly the same number as the search UI (10,752)
    params: dict[str, Any] = {
        "page_size": page_size,
        # only include collections with granules (https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#c-has-granules)
        "has_granules": True,
        "sort_key[]": "-usage_score",
        # subset collections to those which are identifiably from a DAAC https://cmr.earthdata.nasa.gov/search/providers
        "provider[]": get_eosdis_shortnames(),
        # only include collections that are cloud hosted
        "cloud_hosted": True,
    }

    # Add concept_id parameter if provided for debugging
    if concept_id:
        params["concept_id"] = concept_id

    # Add page_num for pagination
    if page_num is not None:
        params["page_num"] = page_num

    try:
        data = await _get_cmr_json(COLLECTIONS_SEARCH_URL, params, client, timeout)
        total_hits = data.get("hits", 0)
        logger.info("Total hits: %s", total_hits)
        return data.get("items", []), total_hits
    except HTTPError as e:
        logger.error("Error fetching collections from CMR: %s", e)
        raise


async def fetch_cmr_collections_by_concept_ids(
    collection_concept_ids: Sequence[str],
    batch_size: int = 1000,
    client: AsyncClient | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[dict[str, Any]]:
    """Fetch eligible CMR collections for explicit concept IDs with POST.

    CMR accepts repeated ``concept_id`` form fields. Using POST avoids URL length
    limits when reprocessing hundreds or thousands of known collection IDs. CMR
    does not guarantee result order, so this returns collections in first-seen
    input order after removing duplicate and blank IDs.

    Args:
        collection_concept_ids: Collection concept IDs to fetch.
        batch_size: Maximum number of IDs to include in each CMR POST request.
        client: Optional shared HTTP client.
        timeout: Request timeout in seconds.

    Returns:
        Collection metadata dictionaries in input concept ID order.

    Raises:
        ValueError: If ``batch_size`` is less than one or if any requested ID
            does not return an eligible CMR collection record.
        httpx2.HTTPError: If a CMR request fails.
    """
    if batch_size < 1:
        msg = "batch_size must be greater than 0"
        raise ValueError(msg)

    concept_ids = _deduplicate_concept_ids(collection_concept_ids)
    if not concept_ids:
        return []

    collections_by_id: dict[str, dict[str, Any]] = {}
    for start in range(0, len(concept_ids), batch_size):
        batch = concept_ids[start : start + batch_size]
        form_data = _collection_search_form_data(batch)
        data = await _post_cmr_json(COLLECTIONS_SEARCH_URL, form_data, client, timeout)
        for collection in data.get("items", []):
            concept_id = collection.get("meta", {}).get("concept-id")
            if isinstance(concept_id, str) and concept_id in batch:
                collections_by_id[concept_id] = collection

    missing_concept_ids = [
        concept_id for concept_id in concept_ids if concept_id not in collections_by_id
    ]
    if missing_concept_ids:
        missing = ", ".join(missing_concept_ids)
        msg = f"No eligible CMR collection found for concept ID(s): {missing}"
        raise ValueError(msg)

    return [collections_by_id[concept_id] for concept_id in concept_ids]


async def fetch_eligible_cmr_collections(
    limit: int | None = None,
    page_size: int = 100,
    client: AsyncClient | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> list[dict[str, Any]]:
    """Fetch eligible CMR collections in usage-score order.

    This pages the same CMR collection search used by
    :func:`fetch_cmr_collections` and keeps only records with a
    ``meta.concept-id`` because batch assessment uses that value as the output
    primary key.

    Args:
        limit: Optional maximum number of collections to return. When omitted,
            all matching collections are fetched.
        page_size: Number of collections to request per CMR page.
        client: Optional shared HTTP client.
        timeout: Request timeout in seconds.

    Returns:
        Collection metadata dictionaries with concept IDs, preserving CMR order.

    Raises:
        ValueError: If ``limit`` is negative or ``page_size`` is less than one.
        httpx2.HTTPError: If a CMR page request fails.
    """
    if limit is not None and limit < 0:
        msg = "limit must be greater than or equal to 0"
        raise ValueError(msg)
    if page_size < 1:
        msg = "page_size must be greater than 0"
        raise ValueError(msg)
    if limit == 0:
        return []

    collections: list[dict[str, Any]] = []
    page_num = 1
    total_hits: int | None = None

    while limit is None or len(collections) < limit:
        page, total_hits = await fetch_cmr_collections(
            page_size=page_size,
            page_num=page_num,
            client=client,
            timeout=timeout,
        )
        if not page:
            break

        for collection in page:
            concept_id = collection.get("meta", {}).get("concept-id")
            if not concept_id:
                logger.warning("Skipping CMR collection without meta.concept-id")
                continue
            collections.append(collection)
            if limit is not None and len(collections) >= limit:
                return collections

        if total_hits is not None and page_num * page_size >= total_hits:
            break
        page_num += 1

    return collections


async def fetch_random_granule_metadata(
    concept_id: str,
    client: AsyncClient | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> tuple[dict[str, Any] | None, int | None]:
    """
    Fetch random granule metadata for a given collection concept ID.

    This function first determines the total number of granules, then randomly
    selects one by offset to avoid bias toward the first granules.

    Args:
        concept_id: Collection concept ID.
        client: Optional shared HTTP client.
        timeout: Request timeout in seconds.

    Returns:
        Tuple of (random granule metadata dictionary, total granule count), or
        (None, None) if no granules are found.

    Raises:
        httpx2.HTTPError: If the API request fails.
    """
    params: dict[str, Any] = {
        "collection_concept_id": concept_id,
        "page_size": 1,
    }

    try:
        # First request to get total count
        data = await _get_cmr_json(GRANULES_SEARCH_URL, params, client, timeout)
        total_num_granules = data.get("hits")

        if not total_num_granules:
            return None, None

        # CMR has a 1 million item pagination limit
        params["offset"] = random.randint(0, min(total_num_granules - 1, int(1e6)))
        # Second request to fetch the random granule
        data = await _get_cmr_json(GRANULES_SEARCH_URL, params, client, timeout)
        granules = data.get("items", [])

        if granules:
            return granules[0], total_num_granules
        return None, None
    except HTTPError as e:
        logger.debug(
            "Error fetching granule metadata for collection %s: %s",
            concept_id,
            e,
            exc_info=True,
        )
        raise


async def fetch_sample_granule_ur(
    collection_concept_id: str,
    client: AsyncClient | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> str | None:
    """Fetch a sampled granule UR for a collection concept ID.

    Args:
        collection_concept_id: CMR collection concept ID.
        client: Optional shared HTTP client.
        timeout: Request timeout in seconds.

    Returns:
        A sampled UMM granule UR, or None if CMR returns no granule or the
        granule metadata does not contain a GranuleUR.
    """
    granule, _ = await fetch_random_granule_metadata(
        collection_concept_id, client=client, timeout=timeout
    )
    if not granule:
        return None

    granule_ur = granule.get("umm", {}).get("GranuleUR")
    if isinstance(granule_ur, str) and granule_ur:
        return granule_ur
    return None


async def fetch_granule_by_ur(
    collection_concept_id: str,
    granule_ur: str,
    client: AsyncClient | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any] | None:
    """
    Fetch granule metadata by collection concept ID and granule UR.

    Args:
        collection_concept_id: CMR collection concept ID.
        granule_ur: CMR granule UR.
        client: Optional shared HTTP client.
        timeout: Request timeout in seconds.

    Returns:
        Granule metadata dictionary or None if not found.

    Raises:
        httpx2.HTTPError: If the API request fails.
    """
    params = {
        "collection_concept_id": collection_concept_id,
        "granule_ur": granule_ur,
        "page_size": 1,
    }

    try:
        data = await _get_cmr_json(GRANULES_SEARCH_URL, params, client, timeout)
        granules = data.get("items", [])

        if granules:
            return granules[0]

        logger.warning(
            "No granule found with UR %s in collection %s",
            granule_ur,
            collection_concept_id,
        )
        return None
    except HTTPError as e:
        logger.error(
            "Error fetching granule metadata for UR %s in collection %s: %s",
            granule_ur,
            collection_concept_id,
            e,
        )
        raise


async def fetch_granule_by_id(
    granule_id: str,
    client: AsyncClient | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any] | None:
    """
    Fetch granule metadata by granule concept ID.

    Args:
        granule_id: Granule concept ID.
        client: Optional shared HTTP client.
        timeout: Request timeout in seconds.

    Returns:
        Granule metadata dictionary or None if not found.

    Raises:
        httpx2.HTTPError: If the API request fails.
    """
    params = {
        "concept_id": granule_id,
        "page_size": 1,
    }

    try:
        data = await _get_cmr_json(GRANULES_SEARCH_URL, params, client, timeout)
        granules = data.get("items", [])

        if granules:
            return granules[0]

        logger.warning("No granule found with ID %s", granule_id)
        return None
    except HTTPError as e:
        logger.error(
            "Error fetching granule metadata for granule %s: %s", granule_id, e
        )
        raise


def _deduplicate_concept_ids(concept_ids: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for concept_id in concept_ids:
        clean_concept_id = concept_id.strip()
        if not clean_concept_id or clean_concept_id in seen:
            continue
        seen.add(clean_concept_id)
        unique.append(clean_concept_id)
    return unique


def _collection_search_form_data(concept_ids: Sequence[str]) -> dict[str, Any]:
    return {
        "page_size": len(concept_ids),
        "has_granules": True,
        "cloud_hosted": True,
        "provider[]": get_eosdis_shortnames(),
        "concept_id": list(concept_ids),
    }


async def _get_cmr_json(
    url: str,
    params: dict[str, Any],
    client: AsyncClient | None,
    timeout: int,
) -> dict[str, Any]:
    if client is None:
        async with AsyncClient(timeout=timeout) as cmr_client:
            return await _get_cmr_json_with_client(url, params, cmr_client, timeout)

    return await _get_cmr_json_with_client(url, params, client, timeout)


async def _post_cmr_json(
    url: str,
    data: dict[str, Any],
    client: AsyncClient | None,
    timeout: int,
) -> dict[str, Any]:
    if client is None:
        async with AsyncClient(timeout=timeout) as cmr_client:
            return await _post_cmr_json_with_client(url, data, cmr_client, timeout)

    return await _post_cmr_json_with_client(url, data, client, timeout)


async def _post_cmr_json_with_client(
    url: str,
    data: dict[str, Any],
    client: AsyncClient,
    timeout: int,
) -> dict[str, Any]:
    response = await client.post(
        url,
        data=data,
        headers=CMR_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        msg = f"CMR returned non-object JSON from {url}"
        raise ValueError(msg)
    return payload


async def _get_cmr_json_with_client(
    url: str,
    params: dict[str, Any],
    client: AsyncClient,
    timeout: int,
) -> dict[str, Any]:
    response = await client.get(
        url,
        params=params,
        headers=CMR_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        msg = f"CMR returned non-object JSON from {url}"
        raise ValueError(msg)
    return data
