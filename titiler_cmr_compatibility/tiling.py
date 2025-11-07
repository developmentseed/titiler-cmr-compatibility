"""
Tile generation and testing functionality.

This module provides the GranuleTilingInfo dataclass for managing tile
generation information and testing tile generation functionality.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Tuple, Any, Type

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
from known_variables import known_variables, known_bands

logger = logging.getLogger(__name__)


class IncompatibilityReason(str, Enum):
    """Reasons why tiling might not be compatible with a granule."""
    UNSUPPORTED_FORMAT = "unsupported_format"
    CANT_OPEN_FILE = "cant_open_file"
    TILE_GENERATION_FAILED = "tile_generation_failed"

@dataclass
class GranuleTilingInfo:
    """
    Information needed to generate tiles for a granule.

    This class encapsulates all metadata and configuration needed to generate
    tiles from a CMR granule using either rasterio or xarray backends.
    """

    collection_concept_id: str
    concept_id: Optional[str]
    data_centers: Optional[List[str]] = None
    temporal_extent: Optional[Tuple[str, str]] = None
    data_variables: Optional[List[str]] = None
    backend: Optional[str] = None
    data_url: Optional[str] = None
    format: Optional[str] = None
    extension: Optional[str] = None
    error_message: Optional[str] = None

    # Computed fields (set in __post_init__)
    tiles_url: Optional[str] = None
    reader: Optional[Type] = None
    reader_options: Optional[dict] = None
    variable: Optional[str] = None

    # tiling result fields
    tiling_compatible: bool = False
    incompatible_reason: Optional[IncompatibilityReason] = None

    def __post_init__(self):
        """Initialize computed fields based on backend and data variables."""
        self.tiles_url = self.generate_tiles_url_for_granule()

        if self.backend == "rasterio":
            self.reader = Reader
            self.reader_options = {}
        elif self.backend == "xarray":
            self.reader = XarrayReader
            # Find first known variable for xarray backend
            self.variable = next(
                (item for item in (self.data_variables or []) if item in known_variables),
                None
            ) or self.data_variables[0]
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
            return None

        base_url = (
            f"{TITILER_CMR_ENDPOINT}/tiles/WebMercatorQuad/{tile_z}/{tile_x}/{tile_y}.png"
            f"?concept_id={self.collection_concept_id}&backend={self.backend}"
        )

        if self.backend == "rasterio":
            band = next(
                (item for item in self.data_variables if item in known_bands),
                None
            )
            if band:
                return f"{base_url}&bands={band}"
            elif isinstance(self.data_variables, list):
                return f"{base_url}&bands={self.data_variables[0]}"
            else:
                logger.warning(
                    f"No known band found for rasterio backend in granule {self.concept_id}"
                )
                return None

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
                _, _ = src_dst.tile(**shared_args)
            logger.info(f"Successfully tested tile generation for granule {self.concept_id}")
            self.tiling_compatible = True
            self.incompatible_reason = None
            self.error_message = None
            return True
        except Exception as e:
            error_message = f"Error testing tile generation for granule {self.concept_id}: {e}"
            raise e
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
        }
