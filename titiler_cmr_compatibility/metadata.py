"""
Metadata extraction and processing functions.

This module provides simplified functions to extract collection-level metadata
and create GranuleTilingInfo instances from granule metadata.
"""

import logging
from typing import Optional, List, Dict, Any

from .tiling import GranuleTilingInfo
from .api import fetch_random_granule_metadata

logger = logging.getLogger(__name__)

from earthaccess.daac import DAACS

daac_map = {
    "NASA NSIDC DAAC": "NSIDC",
    "NASA/MSFC/GHRC": "GHRCDAAC",
    "NASA/JPL/PODAAC": "PODAAC",
    "LP DAAC": "LPDAAC",
    "NASA/GSFC/SED/ESD/TISL/GESDISC": "GES_DISC",
    "NASA/GSFC/SED/ESD/GCDC/OB.DAAC": "OBDAAC",
    "SEDAC": "SEDAC",
    "NASA/GSFC/SED/ESD/HBSL/BISB/LAADS": "LAADS",
    "NASA/LARC/SD/ASDC": "ASDC",
    "OBPG": "OBDAAC"
}

for daac_dict in DAACS:
    for cloud_provider in daac_dict['cloud-providers']:
        daac_map[cloud_provider] = daac_dict['short-name']
    for on_prem_provider in daac_dict['on-prem-providers']:
        daac_map[on_prem_provider] = daac_dict['short-name']

def extract_granule_tiling_info(
    granule: Dict[str, Any],
    collection_file_format: Optional[str] = None,
    collection_data_center: Optional[str] = None,
    access_type: str = "direct",
    num_granules: Optional[int] = None,
    processing_level: Optional[str] = None
) -> Optional[GranuleTilingInfo]:
    """
    Generate tiling information for a specific granule.

    This is a simplified factory function that creates a GranuleTilingInfo
    instance. The GranuleTilingInfo class now handles all metadata extraction
    in its __post_init__ method.

    Args:
        granule: Granule metadata dictionary
        collection_file_format: Optional file format from collection metadata
        data_centers: Optional list of data center names
        access_type: Access type for data links ("direct" or "indirect")

    Returns:
        GranuleTilingInfo or None if unable to process
    """
    granule_meta = granule.get("meta", {})

    # Extract collection concept ID
    collection_concept_id = granule_meta.get("collection-concept-id")

    # TODO: Refactor using earthaccess.daac's DAAC map (but we will still probably want some)
    granule_provider = granule_meta.get("provider-id", None)
    # first check if the granule provider is in the daac map
    data_center_short_name = granule_provider if granule_provider in daac_map.values() else daac_map.get(granule_provider, None)
    # if not, fallback to collection data center
    if not data_center_short_name and collection_data_center:
        data_center_short_name = collection_data_center if collection_data_center in daac_map.values() else daac_map.get(collection_data_center, None)

    if not collection_concept_id:
        logger.error("Could not find collection concept ID in granule metadata")
        return None

    logger.info(f"Found collection concept ID: {collection_concept_id}")

    # Create GranuleTilingInfo - it will handle all extraction in __post_init__
    return GranuleTilingInfo(
        granule_metadata=granule,
        collection_concept_id=collection_concept_id,
        collection_file_format=collection_file_format,
        access_type=access_type,
        data_center_short_name=data_center_short_name,
        num_granules=num_granules,
        processing_level=processing_level
    )


def extract_collection_file_format(collection: Dict[str, Any]) -> Optional[str]:
    """
    Extract file format from collection-level metadata.

    Args:
        collection: Collection metadata dictionary

    Returns:
        File format string or None if not found
    """
    umm = collection.get("umm", {})
    archive_info = umm.get("ArchiveAndDistributionInformation", {})
    file_archive_info = archive_info.get("FileArchiveInformation")

    if not file_archive_info:
        return None

    if isinstance(file_archive_info, dict):
        return file_archive_info.get("Format")
    elif isinstance(file_archive_info, list):
        # TODO: handle cases where there are multiple file formats
        return file_archive_info[0].get("Format") if file_archive_info else None

    return None


def extract_data_center(collection: Dict[str, Any]) -> str:
    """
    Extract unique data center names from collection metadata.

    Args:
        collection: Collection metadata dictionary

    Returns:
        List of unique data center short names
    """
    umm = collection.get("umm", {})
    data_centers = umm.get("DataCenters", [])
    data_center_names = []

    for dc in data_centers:
        short_name = dc.get("ShortName", "Unknown")
        if short_name not in data_center_names:
            data_center_names.append(short_name)
    return data_center_names[0]


def extract_processing_level(collection: Dict[str, Any]) -> str:
    """
    Extract unique data center names from collection metadata.

    Args:
        collection: Collection metadata dictionary

    Returns:
        List of unique data center short names
    """
    umm = collection.get("umm", {})
    processing_level = umm.get("ProcessingLevel", {})
    processing_level_id = processing_level.get("Id", None)
    return processing_level_id

def extract_random_granule_info(collection: Dict[str, Any], access_type: Optional[str] = "direct") -> Optional[GranuleTilingInfo]:
    """
    Extract tiling information for a random granule from a collection.

    Args:
        collection: Collection metadata dictionary from CMR UMM JSON

    Returns:
        GranuleTilingInfo for a random granule or None if unable to process
    """
    meta = collection.get("meta", {})
    concept_id = meta.get("concept-id", "Unknown")

    # Extract collection-level metadata
    collection_file_format = extract_collection_file_format(collection)
    collection_data_center = extract_data_center(collection)
    processing_level = extract_processing_level(collection)

    # Fetch and process random granule
    granule, num_granules = fetch_random_granule_metadata(concept_id)
    if not granule:
        logger.warning(f"No granule found for collection {concept_id}")
        return None

    return extract_granule_tiling_info(
        granule=granule,
        collection_file_format=collection_file_format,
        collection_data_center=collection_data_center,
        access_type=access_type,
        num_granules=num_granules,
        processing_level=processing_level
    )
