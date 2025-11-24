"""
Constants for CMR collections processing.

This module defines supported file formats, extensions, and API endpoints.
"""

# API Endpoints
TITILER_CMR_ENDPOINT = "https://staging.openveda.cloud/api/titiler-cmr"
GRANULES_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
COLLECTIONS_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search/collections.umm_json"

# Supported File Formats and Extensions
HDF_FORMATS = ["HDF", "HDF5", "HDF-EOS5"]
HDF_EXTENSIONS = ["hdf", "hdf5", "h5"]

NETCDF_FORMATS = ["NetCDF", "netCDF-4", "netCDFnetCDF-4 classic"]
NETCDF_EXTENSIONS = ["nc", "nc4"]

COG_FORMATS = ["COG"]
COG_EXTENSIONS = ["cog", "tif", "tiff"]

ZARR_FORMATS = ["zarr"]
ZARR_EXTENSIONS = ["zarr"]

# Combined lists
SUPPORTED_FORMATS = HDF_FORMATS + NETCDF_FORMATS + COG_FORMATS + ZARR_FORMATS
SUPPORTED_EXTENSIONS = HDF_EXTENSIONS + NETCDF_EXTENSIONS + COG_EXTENSIONS + ZARR_EXTENSIONS

# Default tile coordinates for testing
DEFAULT_TILE_X = 0
DEFAULT_TILE_Y = 0
DEFAULT_TILE_Z = 0
