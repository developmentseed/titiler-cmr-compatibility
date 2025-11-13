"""
Tile generation and testing functionality.

This module provides the GranuleTilingInfo dataclass for managing tile
generation information and testing tile generation functionality.
"""

import os
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Tuple, Any, Type, Dict

from earthaccess import DataGranule
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import xarray_open_dataset
from rio_tiler.io import Reader
from titiler.xarray.io import Reader as XarrayReader

from .constants import (
    TITILER_CMR_ENDPOINT,
    COG_FORMATS,
    COG_EXTENSIONS,
    DEFAULT_TILE_X,
    DEFAULT_TILE_Y,
    DEFAULT_TILE_Z,
)
from .validation import is_supported_format, is_supported_extension
from .helpers import open_xarray_dataset, open_rasterio_dataset
from .known_variables import known_variables

logger = logging.getLogger(__name__)


class IncompatibilityReason(str, Enum):
    """Reasons why tiling might not be compatible with a granule."""
    UNSUPPORTED_FORMAT = "unsupported_format"
    CANT_OPEN_FILE = "cant_open_file"
    TILE_GENERATION_FAILED = "tile_generation_failed"
    NO_GRANULE_FOUND = "no_granule_found"
    FAILED_TO_EXTRACT = "failed_to_extract"
    TIMEOUT = "timeout"
    CANT_EXTRACT_VARIABLES = "cant_extract_variables"


@dataclass
class GranuleTilingInfo:
    """
    Information needed to generate tiles for a granule.

    This class encapsulates all metadata and configuration needed to generate
    tiles from a CMR granule using either rasterio or xarray backends.

    The class is self-initializing - pass in the raw granule metadata and
    it will extract all necessary information in __post_init__.
    """
    # Required inputs
    collection_concept_id: str

    # Optional
    granule_metadata: Optional[Dict[str, Any]] = None
    num_granules: Optional[int] = None

    # Optional configuration
    collection_file_format: Optional[str] = None
    access_type: str = "direct"  # "direct" or "indirect"
    data_center_short_name: Optional[str] = None

    # Fields extracted from granule metadata (set in __post_init__)
    data_granule: Optional[DataGranule] = field(default=None, init=False)
    concept_id: Optional[str] = field(default=None, init=False)
    data_url: Optional[str] = field(default=None, init=False)
    temporal_extent: Optional[Tuple[str, str]] = field(default=None, init=False)
    data_variables: Optional[List[str]] = field(default=None, init=False)
    backend: Optional[str] = field(default=None, init=False)
    format: Optional[str] = field(default=None, init=False)
    extension: Optional[str] = field(default=None, init=False)

    # Computed fields for tiling
    tiles_url: Optional[str] = field(default=None, init=False)
    reader: Optional[Type] = field(default=None, init=False)
    reader_options: Optional[dict] = field(default=None, init=False)
    variable: Optional[str] = field(default=None, init=False)

    # Status fields
    error_message: Optional[str] = field(default=None, init=True)
    tiling_compatible: bool = field(default=False, init=True)
    incompatible_reason: Optional[IncompatibilityReason] = field(default=None, init=True)

    def __post_init__(self):
        """Initialize computed fields by extracting metadata from granule."""
        # Create DataGranule wrapper
        if self.granule_metadata:
            self.data_granule = DataGranule(self.granule_metadata)

            # Extract basic metadata
            self._extract_concept_id()
            self._extract_data_url()
            self._extract_temporal_extent()
            self._extract_format_and_extension()

            # Validate format support
            if not self._validate_format():
                return  # Stop initialization if format is unsupported

            # Extract data variables and backend
            self._extract_data_variables_and_backend()

            # Setup reader configuration
            self._setup_reader()

        # Generate tiles URL if we have all required info
        if self.backend and self.data_variables:
            self.tiles_url = self.generate_tiles_url_for_granule()

    def _extract_concept_id(self):
        """Extract granule concept ID from metadata."""
        granule_meta = self.granule_metadata.get("meta", {})
        self.concept_id = granule_meta.get("concept-id")

    def _extract_data_url(self):
        """Extract data URL using earthaccess DataGranule.data_links()."""
        try:
            # Get data links with specified access type
            data_links = self.data_granule.data_links(access=self.access_type)
            if data_links:
                self.data_url = data_links[0]
            else:
                # Fall back to "indirect" if "direct" returns nothing
                if self.access_type == "direct":
                    logger.warning(f"No direct access links found for granule {self.concept_id}, trying external")
                    data_links = self.data_granule.data_links(access="external")
                    if data_links:
                        self.data_url = data_links[0]

            if not self.data_url:
                error_msg = "Could not find data URL in granule metadata"
                logger.error(error_msg)
                self.error_message = error_msg
                self.incompatible_reason = IncompatibilityReason.FAILED_TO_EXTRACT
        except Exception as e:
            error_msg = f"Error extracting data URL: {e}"
            logger.error(error_msg)
            self.error_message = error_msg
            self.incompatible_reason = IncompatibilityReason.FAILED_TO_EXTRACT

    def _extract_temporal_extent(self):
        """Extract temporal extent from granule metadata."""
        from .umm_helpers import parse_temporal
        granule_umm = self.granule_metadata.get("umm", {})
        self.temporal_extent = parse_temporal(granule_umm)

    def _extract_format_and_extension(self):
        """Extract file format and extension from granule metadata."""
        # Try collection format first, then granule-level format
        self.format = self.collection_file_format or self._extract_file_format_from_granule()

        # Extract extension from data URL
        if self.data_url:
            self.extension = os.path.splitext(self.data_url)[1].lstrip('.')

    def _extract_file_format_from_granule(self) -> Optional[str]:
        """
        Extract file format from granule-level metadata.

        Returns:
            File format string or None if not found
        """
        umm = self.granule_metadata.get("umm", {})
        data_granule = umm.get("DataGranule", {})
        archive_info = data_granule.get("ArchiveAndDistributionInformation", {})

        # Handle both list and dict formats for ArchiveAndDistributionInformation
        if isinstance(archive_info, list) and archive_info:
            archive_info = archive_info[0]  # Take the first item if it's a list

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

    def _validate_format(self) -> bool:
        """
        Validate if granule format or extension is supported.

        Returns:
            True if supported, False otherwise (sets incompatible_reason)
        """
        if not self.data_url:
            return False

        if self.format is not None:
            is_supported = is_supported_format(self.format)
            error_message = f"Format {self.format} is not supported"
        else:
            is_supported = is_supported_extension(self.extension)
            error_message = f"Extension {self.extension} is not supported"

        if not is_supported:
            logger.warning(error_message)
            self.error_message = error_message
            self.incompatible_reason = IncompatibilityReason.UNSUPPORTED_FORMAT
            return False

        return True

    def _extract_data_variables_and_backend(self):
        """Extract data variables and determine backend (rasterio or xarray)."""
        if not self.data_url:
            return

        try:
            file_format = self.format or self.extension

            if file_format in COG_FORMATS or file_format in COG_EXTENSIONS:
                # Use rasterio backend
                with open_rasterio_dataset(self.data_url, self.data_center_short_name) as src:
                    self.data_variables = src.descriptions
                self.backend = "rasterio"
            else:
                # Use xarray backend
                with open_xarray_dataset(self.data_url, self.data_center_short_name) as ds:
                    self.data_variables = list(ds.data_vars.keys())
                self.backend = "xarray"

            if not self.data_variables:
                error_msg = "Can't extract variables"
                logger.error(error_msg)
                self.error_message = error_msg
                self.incompatible_reason = IncompatibilityReason.CANT_EXTRACT_VARIABLES

        except Exception as e:
            error_msg = f"Error opening {self.data_url}: {e}"
            logger.error(error_msg)
            self.error_message = error_msg
            self.incompatible_reason = IncompatibilityReason.CANT_OPEN_FILE

    def _setup_reader(self):
        """Setup the appropriate reader and reader options based on backend."""
        if not self.backend or not self.data_variables:
            return

        if self.backend == "rasterio":
            self.reader = Reader
            self.reader_options = {}
        elif self.backend == "xarray":
            self.reader = XarrayReader
            # Find first known variable for xarray backend
            self.variable = next(
                (item for item in (self.data_variables or []) if item in known_variables),
                None
            )
            if not self.variable and len(self.data_variables) > 0:
                self.variable = self.data_variables[0]
            self.reader_options = {
                "variable": self.variable,
                "opener": xarray_open_dataset
            }

    def generate_tiles_url_for_granule(
        self,
        tile_x: int = DEFAULT_TILE_X,
        tile_y: int = DEFAULT_TILE_Y,
        tile_z: int = DEFAULT_TILE_Z
    ) -> Optional[str]:
        """
        Generate a tiles URL for this granule.

        Args:
            tile_x: X coordinate of the tile (default: 0)
            tile_y: Y coordinate of the tile (default: 0)
            tile_z: Zoom level of the tile (default: 0)

        Returns:
            Tiles URL string or None if unable to generate
        """
        if not self.backend or not self.data_variables:
            raise ValueError("Cannot generate tiles URL without backend and data variables")

        base_url = (
            f"{TITILER_CMR_ENDPOINT}/tiles/WebMercatorQuad/{tile_z}/{tile_x}/{tile_y}.png"
            f"?concept_id={self.collection_concept_id}&backend={self.backend}"
        )

        if self.backend == "rasterio":
            return base_url

        elif self.backend == "xarray":
            variable = next(
                (item for item in self.data_variables if item in known_variables),
                None
            )
            if self.temporal_extent:
                datetime_param = '/'.join(self.temporal_extent)
                if variable:
                    return f"{base_url}&variable={variable}&datetime={datetime_param}"
                elif isinstance(self.data_variables, list):
                    return f"{base_url}&variable={self.data_variables[0]}&datetime={datetime_param}"
            else:
                if not variable:
                    logger.warning(
                        f"No known variable found for xarray backend in granule {self.concept_id}"
                    )
                if not self.temporal_extent:
                    logger.warning(
                        f"No temporal extent found for granule {self.concept_id}"
                    )
                return None

        return None

    def test_tiling(
        self,
        auth: Any,
        tile_x: int = DEFAULT_TILE_X,
        tile_y: int = DEFAULT_TILE_Y,
        tile_z: int = DEFAULT_TILE_Z
    ) -> bool:
        """
        Test tile generation for this granule.

        Args:
            auth: Authentication object from earthaccess
            tile_x: X coordinate of the tile (default: 0)
            tile_y: Y coordinate of the tile (default: 0)
            tile_z: Zoom level of the tile (default: 0)

        Returns:
            True if tile generation succeeded, False otherwise
        """
        cmr_query = {
            "concept_id": self.collection_concept_id,
            "temporal": self.temporal_extent,
        }

        shared_args = {
            "tile_x": tile_x,
            "tile_y": tile_y,
            "tile_z": tile_z,
            "cmr_query": cmr_query
        }

        try:
            with CMRBackend(
                reader=self.reader,
                auth=auth,
                reader_options=self.reader_options or {},
            ) as src_dst:
                _ = src_dst.tile(**shared_args)
            logger.info(f"Successfully tested tile generation for granule {self.concept_id}")
            self.tiling_compatible = True
            self.incompatible_reason = None
            self.error_message = None
            return True
        except Exception as e:
            error_message = f"Error testing tile generation for granule {self.concept_id}: {e}"
            logger.error(error_message)
            self.tiling_compatible = False
            self.error_message = str(e)
            self.incompatible_reason = IncompatibilityReason.TILE_GENERATION_FAILED
            return False

    def to_report_dict(self) -> dict:
        """
        Convert tiling info to a dictionary suitable for assessment reports.

        Returns:
            Dictionary containing key fields for reporting
        """
        return {
            "collection_concept_id": self.collection_concept_id,
            "concept_id": self.concept_id,
            "data_center": self.data_center_short_name,
            "data_url": self.data_url,
            "backend": self.backend,
            "format": self.format,
            "extension": self.extension,
            "tiling_compatible": self.tiling_compatible,
            "incompatible_reason": self.incompatible_reason.value if self.incompatible_reason else None,
            "error_message": self.error_message,
            "tiles_url": self.tiles_url,
            "variable": self.variable,
            "data_variables": self.data_variables,
            "num_granules": self.num_granules
        }
