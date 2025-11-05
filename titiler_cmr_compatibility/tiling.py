"""
Tile generation and testing functionality.

This module provides the GranuleTilingInfo dataclass for managing tile
generation information and testing tile generation functionality.
"""

import logging
from dataclasses import dataclass
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


@dataclass
class GranuleTilingInfo:
    """
    Information needed to generate tiles for a granule.

    This class encapsulates all metadata and configuration needed to generate
    tiles from a CMR granule using either rasterio or xarray backends.
    """

    collection_concept_id: str
    concept_id: str
    data_centers: Optional[List[str]] = None
    temporal_extent: Optional[Tuple[str, str]] = None
    data_variables: Optional[List[str]] = None
    backend: Optional[str] = None
    data_url: Optional[str] = None
    format: Optional[str] = None
    extension: Optional[str] = None

    # Computed fields (set in __post_init__)
    tiles_url: Optional[str] = None
    reader: Optional[Type] = None
    reader_options: Optional[dict] = None
    variable: Optional[str] = None

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
            )
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
                self.data_variables[0]
            )
            if band:
                return f"{base_url}&bands={band}"
            else:
                logger.warning(
                    f"No known band found for rasterio backend in granule {self.concept_id}"
                )
                return None

        elif self.backend == "xarray":
            variable = next(
                (item for item in self.data_variables if item in known_variables),
                self.data_variables[0]
            )
            if variable and self.temporal_extent:
                datetime_param = '/'.join(self.temporal_extent)
                return f"{base_url}&variable={variable}&datetime={datetime_param}"
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

        Raises:
            Exception: If tile generation fails
        """
        if not self.reader or not self.temporal_extent:
            logger.error("Cannot test tiling: missing reader or temporal extent")
            return False

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
                image, _ = src_dst.tile(**shared_args)
            logger.info(f"Successfully tested tile generation for granule {self.concept_id}")
            return True
        except Exception as e:
            logger.error(f"Error testing tile generation for granule {self.concept_id}: {e}")
            raise
