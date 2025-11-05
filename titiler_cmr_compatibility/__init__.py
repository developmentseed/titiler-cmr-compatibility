"""
CMR Collections Processing Package

A package for interacting with NASA's Common Metadata Repository (CMR)
to fetch, process, and generate tiles from collections and granules.

Modules:
    api: CMR API interaction functions
    constants: Configuration constants and supported formats
    metadata: Metadata extraction and processing
    tiling: Tile generation and testing
    validation: Format and extension validation
    cli: Command-line interface

Example usage:
    >>> from cmr_collections import fetch_cmr_collections, extract_random_granule_info
    >>> collections = fetch_cmr_collections(page_size=10)
    >>> for collection in collections:
    ...     info = extract_random_granule_info(collection)
    ...     print(info.tiles_url)
"""

# API functions
from .api import (
    fetch_cmr_collections,
    fetch_random_granule_metadata,
    fetch_granule_by_id,
)

# Validation functions
from .validation import (
    is_supported,
    is_supported_format,
    is_supported_extension,
)

# Metadata functions
from .metadata import (
    extract_file_format_from_granule_metadata,
    get_data_url,
    extract_data_variables,
    extract_granule_tiling_info,
    extract_collection_file_format,
    extract_data_centers,
    extract_random_granule_info,
)

# Tiling classes and functions
from .tiling import GranuleTilingInfo

# Constants
from .constants import (
    TITILER_CMR_ENDPOINT,
    GRANULES_SEARCH_URL,
    COLLECTIONS_SEARCH_URL,
    HDF_FORMATS,
    HDF_EXTENSIONS,
    NETCDF_FORMATS,
    NETCDF_EXTENSIONS,
    COG_FORMATS,
    COG_EXTENSIONS,
    ZARR_FORMATS,
    ZARR_EXTENSIONS,
    SUPPORTED_FORMATS,
    SUPPORTED_EXTENSIONS,
    DEFAULT_TILE_X,
    DEFAULT_TILE_Y,
    DEFAULT_TILE_Z,
)

__version__ = "1.0.0"

__all__ = [
    # API
    "fetch_cmr_collections",
    "fetch_random_granule_metadata",
    "fetch_granule_by_id",
    # Validation
    "is_supported",
    "is_supported_format",
    "is_supported_extension",
    # Metadata
    "extract_file_format_from_granule_metadata",
    "get_data_url",
    "extract_data_variables",
    "extract_granule_tiling_info",
    "extract_collection_file_format",
    "extract_data_centers",
    "extract_random_granule_info",
    # Tiling
    "GranuleTilingInfo",
    # Constants
    "TITILER_CMR_ENDPOINT",
    "GRANULES_SEARCH_URL",
    "COLLECTIONS_SEARCH_URL",
    "HDF_FORMATS",
    "HDF_EXTENSIONS",
    "NETCDF_FORMATS",
    "NETCDF_EXTENSIONS",
    "COG_FORMATS",
    "COG_EXTENSIONS",
    "ZARR_FORMATS",
    "ZARR_EXTENSIONS",
    "SUPPORTED_FORMATS",
    "SUPPORTED_EXTENSIONS",
    "DEFAULT_TILE_X",
    "DEFAULT_TILE_Y",
    "DEFAULT_TILE_Z",
]
