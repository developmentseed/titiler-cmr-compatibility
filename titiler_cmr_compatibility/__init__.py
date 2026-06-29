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
    >>> import asyncio
    >>> from titiler_cmr_compatibility import fetch_cmr_collections, extract_random_granule_info
    >>> collections, _ = asyncio.run(fetch_cmr_collections(page_size=10))
    >>> for collection in collections:
    ...     info = asyncio.run(extract_random_granule_info(collection))
    ...     print(info.tiles_url)
"""

# API functions
from .api import (
    fetch_cmr_collections,
    fetch_cmr_collections_by_concept_ids,
    fetch_eligible_cmr_collections,
    fetch_random_granule_metadata,
    fetch_sample_granule_ur,
    fetch_granule_by_id,
    fetch_granule_by_ur,
)

# Validation functions
from .validation import (
    is_supported,
    is_supported_format,
    is_supported_extension,
)

# Metadata and tiling functions require the optional titiler dependency.
try:
    from .metadata import (
        extract_granule_tiling_info,
        extract_collection_file_format,
        extract_data_center,
        extract_random_granule_info,
    )
    from .tiling import GranuleTilingInfo
except ModuleNotFoundError as exc:
    if exc.name != "titiler":
        raise
    extract_granule_tiling_info = None
    extract_collection_file_format = None
    extract_data_center = None
    extract_random_granule_info = None
    GranuleTilingInfo = None

# API-first assessment functions
from .assessment import assess_collection_compatibility
from .assessment_runs import (
    AssessmentProgressEvent,
    assess_collections,
    run_batch_assessment,
    write_assessment_parquet,
)

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
    "fetch_eligible_cmr_collections",
    "fetch_random_granule_metadata",
    "fetch_sample_granule_ur",
    "fetch_granule_by_id",
    "fetch_granule_by_ur",
    # Validation
    "is_supported",
    "is_supported_format",
    "is_supported_extension",
    # Metadata
    "extract_granule_tiling_info",
    "extract_collection_file_format",
    "extract_data_center",
    "extract_random_granule_info",
    # Tiling
    "GranuleTilingInfo",
    # API-first assessment
    "AssessmentProgressEvent",
    "assess_collection_compatibility",
    "assess_collections",
    "fetch_cmr_collections_by_concept_ids",
    "run_batch_assessment",
    "write_assessment_parquet",
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
