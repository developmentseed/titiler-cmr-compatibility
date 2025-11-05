"""
Metadata extraction and processing functions.

This module provides functions to extract and process metadata from CMR
collections and granules, including file formats, URLs, and data variables.
"""

import os
import logging
from typing import Optional, List, Dict, Any, Tuple

from helpers import open_xarray_dataset, open_rasterio_dataset
from umm_helpers import parse_temporal, parse_bounds_from_spatial

from .validation import is_supported_format, is_supported_extension
from .constants import COG_FORMATS, COG_EXTENSIONS
from .tiling import GranuleTilingInfo
from .api import fetch_random_granule_metadata

logger = logging.getLogger(__name__)


def extract_file_format_from_granule_metadata(
    granule_metadata: Dict[str, Any]
) -> Optional[str]:
    """
    Extract file format from granule metadata.

    First checks DataGranule/ArchiveAndDistributionInformation/Format,
    then falls back to None if not found.

    Args:
        granule_metadata: Granule metadata dictionary

    Returns:
        File format string or None if not found
    """
    umm = granule_metadata.get("umm", {})
    data_granule = umm.get("DataGranule", {})
    archive_info = data_granule.get("ArchiveAndDistributionInformation", {})

    # Handle both list and dict formats for ArchiveAndDistributionInformation
    if isinstance(archive_info, list) and archive_info:
        archive_info = archive_info[0]  # Take the first item if it's a list

    # It seems very rare to have the format provided
    if isinstance(archive_info, list) and archive_info:
        format_info = archive_info[0]
        fmt = format_info.get("Format")
        if fmt:
            return fmt
    elif isinstance(archive_info, dict):
        fmt = archive_info.get("Format")
        if fmt:
            return fmt

    return None


def get_data_url(granule: Dict[str, Any]) -> Optional[str]:
    """
    Get the data URL from granule metadata.

    Args:
        granule: Granule metadata dictionary

    Returns:
        Data URL string or None if not found
    """
    umm = granule.get("umm", {})
    related_urls = umm.get("RelatedUrls", [])
    data_urls = []

    for url_info in related_urls:
        url_type = url_info.get("Type")
        # TODO(medium): prefer GET DATA VIA DIRECT ACCESS if available
        if url_type in ["GET DATA"]:  # , "GET DATA VIA DIRECT ACCESS"]:
            data_urls.append(url_info)

    url = None
    if len(data_urls) == 1:
        url = data_urls[0].get("URL")
    elif len(data_urls) > 1:
        for url_info in data_urls:
            subtype = url_info.get("Subtype")
            # None is for cases where the subtype is not present, we just take the first one
            if subtype in ["DIRECT DOWNLOAD", "VIRTUAL COLLECTION", None]:
                url = url_info.get("URL")
                break

    return url if url else None


def extract_data_variables(
    data_url: str,
    file_format: str,
    data_center_name: str
) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Extract data variables from a data URL using either rasterio or xarray.

    Args:
        data_url: URL to the data file
        file_format: File format string
        data_center_name: Data center identifier for authentication

    Returns:
        Tuple of (backend_name, list_of_variables) or (None, None) on error
    """
    data_variables = []
    try:
        if file_format in COG_FORMATS or file_format in COG_EXTENSIONS:
            with open_rasterio_dataset(data_url, data_center_name) as src:
                data_variables = src.descriptions
            return "rasterio", data_variables
        else:
            ds = open_xarray_dataset(data_url, data_center_name)
            data_variables = list(ds.data_vars.keys())
            return "xarray", data_variables
    except Exception as e:
        logger.error(f"Error opening file {data_url}: {e}")
        return None, None


def _validate_granule_format(
    granule_format: Optional[str],
    granule_extension: str
) -> Tuple[bool, str]:
    """
    Validate if granule format or extension is supported.

    Args:
        granule_format: Optional file format string
        granule_extension: File extension string

    Returns:
        Tuple of (is_supported, error_message)
    """
    if granule_format is not None:
        is_supported = is_supported_format(granule_format)
        error_message = f"Format {granule_format} is not supported"
    else:
        is_supported = is_supported_extension(granule_extension)
        error_message = f"Extension {granule_extension} is not supported"

    return is_supported, error_message


def _create_unsupported_granule_info(
    granule: Dict[str, Any],
    collection_concept_id: str,
    granule_data_url: str,
    granule_format: Optional[str],
    granule_extension: str
) -> GranuleTilingInfo:
    """
    Create a GranuleTilingInfo for an unsupported format.

    Args:
        granule: Granule metadata dictionary
        collection_concept_id: Collection concept ID
        granule_data_url: URL to granule data
        granule_format: Optional file format
        granule_extension: File extension

    Returns:
        GranuleTilingInfo with limited information
    """
    granule_umm = granule.get("umm", {})
    return GranuleTilingInfo(
        collection_concept_id=collection_concept_id,
        concept_id=granule_umm.get("GranuleUR", "Unknown"),
        data_url=granule_data_url,
        format=granule_format,
        extension=granule_extension
    )


def _create_supported_granule_info(
    granule: Dict[str, Any],
    collection_concept_id: str,
    granule_data_url: str,
    granule_format: Optional[str],
    granule_extension: str,
    data_centers: Optional[List[str]]
) -> Optional[GranuleTilingInfo]:
    """
    Create a GranuleTilingInfo for a supported format.

    Args:
        granule: Granule metadata dictionary
        collection_concept_id: Collection concept ID
        granule_data_url: URL to granule data
        granule_format: Optional file format
        granule_extension: File extension
        data_centers: List of data center names

    Returns:
        GranuleTilingInfo or None if data extraction fails
    """
    granule_umm = granule.get("umm", {})
    granule_meta = granule.get("meta", {})

    # Get data center name
    data_center_name = granule_meta.get("provider-id")

    # Extract data variables
    backend, data_variables = extract_data_variables(
        granule_data_url,
        granule_format or granule_extension,
        data_center_name
    )

    error_message = None
    if not backend or not data_variables:
        error_message = f"Could not extract data variables for granule {granule_umm.get('GranuleUR')}"
        logger.error(error_message)

    temporal_extent = parse_temporal(granule_umm)

    return GranuleTilingInfo(
        collection_concept_id=collection_concept_id,
        concept_id=granule_umm.get("GranuleUR", "Unknown"),
        data_centers=data_centers,
        temporal_extent=temporal_extent,
        data_variables=data_variables,
        backend=backend,
        data_url=granule_data_url,
        format=granule_format,
        extension=granule_extension,
        error_message=error_message
    )


def extract_granule_tiling_info(
    granule: Dict[str, Any],
    collection_file_format: Optional[str] = None,
    data_centers: Optional[List[str]] = None
) -> Optional[GranuleTilingInfo]:
    """
    Generate tiling information for a specific granule.

    Args:
        granule: Granule metadata dictionary
        collection_file_format: Optional file format from collection metadata
        data_centers: Optional list of data center names

    Returns:
        GranuleTilingInfo or None if unable to process
    """
    granule_umm = granule.get("umm", {})
    granule_meta = granule.get("meta", {})

    # Extract collection concept ID
    collection_concept_id = granule_meta.get("collection-concept-id")
    if not collection_concept_id:
        logger.error("Could not find collection concept ID in granule metadata")
        return None

    logger.info(f"Found collection concept ID: {collection_concept_id}")

    # Get data URL and format
    granule_data_url = get_data_url(granule)
    if not granule_data_url:
        logger.error("Could not find data URL in granule metadata")
        return None

    granule_format = collection_file_format or extract_file_format_from_granule_metadata(granule)
    granule_extension = os.path.splitext(granule_data_url)[1].lstrip('.')

    # Validate format support
    is_supported, error_message = _validate_granule_format(granule_format, granule_extension)

    if not is_supported:
        logger.warning(error_message)
        return _create_unsupported_granule_info(
            granule,
            collection_concept_id,
            granule_data_url,
            granule_format,
            granule_extension
        )

    # Process supported format
    return _create_supported_granule_info(
        granule,
        collection_concept_id,
        granule_data_url,
        granule_format,
        granule_extension,
        data_centers
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


def extract_data_centers(collection: Dict[str, Any]) -> List[str]:
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

    return data_center_names


def extract_random_granule_info(collection: Dict[str, Any]) -> Optional[GranuleTilingInfo]:
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
    data_center_names = extract_data_centers(collection)

    # Fetch and process random granule
    try:
        granule = fetch_random_granule_metadata(concept_id)
        if not granule:
            logger.warning(f"No granule found for collection {concept_id}")
            return None

        return extract_granule_tiling_info(granule, collection_file_format, data_center_names)
    except Exception as e:
        logger.error(f"Error processing granule for collection {concept_id}: {e}")
        return None
