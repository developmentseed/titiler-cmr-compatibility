import requests
import json
import os
import earthaccess
import random
import argparse
from typing import Optional, List, Dict, Any, Tuple, TypedDict
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import MultiFilesBandsReader, xarray_open_dataset
from rio_tiler.io import Reader
from titiler.xarray.io import Reader as XarrayReader

from helpers import open_xarray_dataset, open_rasterio_dataset
from umm_helpers import parse_temporal, parse_bounds_from_spatial

titiler_cmr_endpoint = "https://staging.openveda.cloud/api/titiler-cmr"
granules_search_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
x, y, z = 0, 0, 0

hdf_formats = ["HDF", "HDF5", "HDF-EOS5"]
hdf_extensions = ["hdf", "hdf5", "h5"]
netcdf_formats = ["NetCDF", "netCDF-4", "netCDFnetCDF-4 classic"]
netcdf_extensions = ["nc", "nc4"]
cog_formats = ["COG"]
cog_extensions = ["cog", "tif", "tiff"]
zarr_formats = ["zarr"]
zarr_extensions = ["zarr"]
supported_formats = hdf_formats + netcdf_formats + cog_formats + zarr_formats
supported_extensions = hdf_extensions + netcdf_extensions + cog_extensions + zarr_extensions

from known_variables import known_variables, known_bands

def fetch_cmr_collections(page_size: int = 10, concept_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch collections from CMR in UMM JSON format.

    Args:
        page_size: Number of collections to retrieve per request
        concept_id: Optional specific collection concept ID to search for (for debugging)

    Returns:
        List of collection metadata dictionaries
    """
    url = "https://cmr.earthdata.nasa.gov/search/collections.umm_json"
    # these params return matching collections exactly the same number as the search UI (10,752)
    params = {
        "page_size": page_size,
        "has_granules_or_cwic": "true",
        "sort_key[]": "-usage_score",
        "processing_level_id[]": ["3", "4"],
    }

    # Add concept_id parameter if provided for debugging
    if concept_id:
        params["concept_id"] = concept_id

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        print(f"Total hits: {data.get('hits')}")
        return data.get("items", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching collections from CMR: {e}")
        return []

def fetch_random_granule_metadata(concept_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the random granule metadata for a given collection concept ID.

    Args:
        concept_id: Collection concept ID

    Returns:
        First granule metadata dictionary or None if no granules found
    """
    params = {
        "collection_concept_id": concept_id,
        "page_size": 1
    }

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        response = requests.get(granules_search_url, params=params, headers=headers, timeout=30)
        total_num_granules = response.json().get("hits")
        # You can not page past the 1 millionth item.
        params["offset"] = random.randint(0, min(total_num_granules, int(1e6)))
        response = requests.get(granules_search_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        granules = data.get("items", [])

        if granules:
            return granules[0]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching granule metadata for collection {concept_id}: {e}")
        return None

def fetch_granule_by_id(granule_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch granule metadata by granule concept ID.

    Args:
        granule_id: Granule concept ID

    Returns:
        Granule metadata dictionary or None if not found
    """
    params = {
        "concept_id": granule_id,
        "page_size": 1
    }

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        response = requests.get(granules_search_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        granules = data.get("items", [])

        if granules:
            return granules[0]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching granule metadata for granule {granule_id}: {e}")
        return None

def extract_file_format_from_granule_metadata(granule_metadata: Dict[str, Any]) -> Optional[str]:
    """
    Extract file format from granule metadata.
    First checks DataGranule/ArchiveAndDistributionInformation/Format,
    then falls back to URL suffix analysis.

    Args:
        granule: Granule metadata dictionary

    Returns:
        File format string or None if not found
    """
    umm = granule_metadata.get("umm", {})

    # First, try to get format from DataGranule/ArchiveAndDistributionInformation/Format
    data_granule = umm.get("DataGranule", {})
    archive_info = data_granule.get("ArchiveAndDistributionInformation", {})

    # Handle both list and dict formats for ArchiveAndDistributionInformation
    if isinstance(archive_info, list) and archive_info:
        archive_info = archive_info[0]  # Take the first item if it's a list

    # it seems very rare to have the format provided
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


def is_supported_format(file_format: str) -> bool:
    """
    Check if the file format is supported for xarray opening.

    Args:
        file_format: File format string

    Returns:
        True if format is supported, False otherwise
    """
    format_lower = file_format.lower()

    # Direct format name matches
    if format_lower in [fmt.lower() for fmt in supported_formats]:
        return True

    # Check if the format is a supported extension
    if format_lower in [fmt.lower() for fmt in supported_extensions]:
        return True

    return False

def is_supported_extension(file_ext: str) -> bool:
    """
    Check if the file extension may be supported

    Args:
        file_ext: File extension string

    Returns:
        True if format is supported, False otherwise
    """
    ext_lower = file_ext.lower()

    if ext_lower in [ext.lower() for ext in supported_extensions]:
        return True

    return False

def get_data_url(granule: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    Get the data URL from granule metadata.
    """
    umm = granule.get("umm", {})
    related_urls = umm.get("RelatedUrls", [])
    data_urls = []
    for url_info in related_urls:
        url_type = url_info.get("Type")
        # TODO(medium): prefer GET DATA VIA DIRECT ACCESS if available
        if url_type in ["GET DATA", "GET DATA VIA DIRECT ACCESS"]:
            data_urls.append(url_info)

    if len(data_urls) == 1:
        url = data_urls[0].get("URL")
    elif len(data_urls) > 1:
        for url_info in data_urls:
            subtype = url_info.get("Subtype")
            # None is for cases where the subtype is not present, we just take the first one
            if subtype in ["DIRECT DOWNLOAD", "VIRTUAL COLLECTION", None]:
                url = url_info.get("URL")
    return url if url else None

def extract_data_variables(data_url: str, file_format: str, data_center_name: str) -> Tuple[str, Optional[List[str]]]:
    """
    Extract data variables from a data URL.
    """
    try:
        if file_format in cog_formats or file_format in cog_extensions:
            with open_rasterio_dataset(data_url, data_center_name) as src:
                data_variables = "rasterio", src.descriptions
            if not data_variables:
                data_variables = "rasterio", ["could not extract data variables using rasterio"]
        else:
            ds = open_xarray_dataset(data_url, data_center_name)
            data_variables = "xarray", list(ds.data_vars.keys())
            if not data_variables:
                data_variables = "xarray", ["could not extract data variables using xarray"]
        return data_variables
    except Exception as e:
        print(f"Error opening file {data_url}: {e}")
        return None, None

class GranuleTilingInfo(TypedDict):
    collection_concept_id: str
    concept_id: str
    data_centers: Optional[list[str]]
    temporal_extent: Optional[tuple]
    data_variables: Optional[list[str]]
    backend: Optional[str]
    granule_format: Optional[str]
    granule_extension: Optional[str]

    def __attrs_post_init__(self):
        self.tiles_url = self.generate_tiles_url_for_granule()
        if self.backend == "rasterio":
            self.reader = Reader
            self.reader_options = {}
        elif self.backend == "xarray":
            self.reader = XarrayReader
            self.variable = next((item for item in self.data_variables if item in known_variables), None)
            self.reader_options = {
                "variable": self.variable,
                "opener": xarray_open_dataset
            }

    def test_tiling(self):
        cmr_query = {
            "concept_id": self.collection_concept_id,
            "temporal": self.temporal_extent,
        }
        shared_args = {
            "tile_x": x,
            "tile_y": y,
            "tile_z": z,
            "cmr_query": cmr_query
        }
        # Test the tile generation
        try:
            with CMRBackend(
                reader=self.reader,
                auth=auth,
                reader_options=self.reader_options,
            ) as src_dst:
                image, _ = src_dst.tile(**shared_args)
            print("✓ Successfully tested tile generation")
        except Exception as e:
            print(f"✗ Error testing tile generation: {e}")
            raise e
            # return None

    def generate_tiles_url_for_granule(self) -> Optional[str]:
        # Generate tiles URL
        tiles_url = None

        if self.backend == "rasterio":
            band = next((item for item in granule_info,data_variables if item in known_bands), None)
            if band:
                tiles_url = f"{titiler_cmr_endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png?concept_id={self.collection_concept_id}&backend={self.backend}&bands={band}"
            else:
                print("No known band found for rasterio backend")
        elif self.backend == "xarray":
            variable = next((item for item in self.data_variables if item in known_variables), None)
            if variable:
                tiles_url = f"{titiler_cmr_endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png?concept_id={self.collection_concept_id}&backend={self.backend}&variable={variable}&datetime={('/').join(self.temporal_extent)}"
            else:
                print("No known variable found for xarray backend")

        return tiles_url

def extract_granule_tiling_info(granule: Dict[str, Any], collection_file_format: Optional[str] = None, data_centers: Optional[list[str]] = None) -> Optional[str]:
    """
    Generate a tiles URL for a specific granule.

    Args:
        granule_id: Granule concept ID

    Returns:
        Tiles URL string or None if unable to generate
    """
    # Get collection concept ID from granule
    granule_umm = granule.get("umm", {})
    granule_meta = granule.get("meta", {})
    collection_concept_id = granule_meta.get("collection-concept-id", None)

    if not collection_concept_id:
        print("Could not find collection concept ID in granule metadata")
        return None

    print(f"Found collection concept ID: {collection_concept_id}")

    # Get data URL and format
    granule_data_url = get_data_url(granule)
    if not granule_data_url:
        print("Could not find data URL in granule metadata")
        return None

    granule_format = collection_file_format or extract_file_format_from_granule_metadata(granule)
    granule_extension = os.path.splitext(granule_data_url)[1].lstrip('.')
    if granule_format is not None:
        is_supported = is_supported_format(granule_format)
        not_supported_message = f"Format {granule_format} is not supported"
    else:
        is_supported = is_supported_extension(granule_extension)
        not_supported_message = f"Extension {granule_extension} is not supported"

    if not is_supported:
        print(not_supported_message)
        return None

    # Get data center name
    data_center_name = granule_meta["provider-id"]

    # Extract data variables
    backend, data_variables = extract_data_variables(granule_data_url, granule_format, data_center_name)
    if not backend or not data_variables:
        print("Could not extract data variables")
        return None

    temporal_extent = parse_temporal(granule_umm)
    return GranuleTilingInfo(
        collection_concept_id=collection_concept_id,
        concept_id=granule_umm.get("concept-id"),
        data_centers=data_centers,
        temporal_extent=temporal_extent,
        data_variables=data_variables,
        backend=backend,
        granule_format=granule_format,
        granule_extension=granule_extension
    )

def extract_random_granule_info(collection: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract relevant metadata from a UMM JSON collection response.

    Args:
        collection: Collection metadata dictionary from CMR UMM JSON

    Returns:
        Dictionary containing extracted collection information
    """
    umm = collection.get("umm", {})

    # Extract concept ID
    concept_id = collection.get("meta", {}).get("concept-id", "Unknown")

    # Extract collection-level file archive format
    collection_file_format = None
    archive_info = umm.get("ArchiveAndDistributionInformation", {})
    file_archive_info = archive_info.get("FileArchiveInformation", None)

    if file_archive_info:
        if isinstance(file_archive_info, dict):
            collection_file_format = file_archive_info.get("Format", "Unknown")
        ## TODO: handle cases where there are multiple file formats
        elif isinstance(file_archive_info, list):
            collection_file_format = file_archive_info[0].get("Format", "Unknown")

    # Extract data center short name
    data_centers = umm.get("DataCenters", [])
    data_center_names = []

    for dc in data_centers:
        short_name = dc.get("ShortName", "Unknown")
        if short_name not in data_center_names:
            data_center_names.append(short_name)

    granule = fetch_random_granule_metadata(concept_id)
    granule_tiling_info = extract_granule_tiling_info(granule, collection_file_format, data_centers)
    return granule_tiling_info


def main():
    """
    Main function to fetch and display collection metadata or generate tiles URL for specific granule.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch and display CMR collection metadata or generate tiles URL for specific granule')
    parser.add_argument('--collection', type=str, help='Specific collection concept ID to search for (for debugging)')
    parser.add_argument('--granule-id', type=str, help='Specific granule concept ID to generate tiles URL for')
    args = parser.parse_args()

    # Handle granule-specific mode
    if args.granule_id:
        print(f"Generating tiles URL for granule ID: {args.granule_id}")
        granule = fetch_granule_by_id(args.granule_id)
        granule_tiling_info = extract_granule_tiling_info(granule)
        tiles_url = granule_tiling_info.generate_tiles_url_for_granule()
        if tiles_url:
            print(f"\n✓ Success! Tiles URL generated:")
            print(f"{tiles_url}")
        else:
            print(f"\n✗ Failed to generate tiles URL for granule {args.granule_id}")
        return

    # Handle collection mode (existing functionality)
    if args.collection:
        print(f"Fetching specific collection {args.collection} from CMR in UMM JSON format...\n")
        collections = fetch_cmr_collections(page_size=1, concept_id=args.collection)
    else:
        print("Fetching collections from CMR in UMM JSON format...\n")
        collections = fetch_cmr_collections(page_size=100)

    if not collections:
        print("No collections retrieved.")
        return

    print(f"Retrieved {len(collections)} collections\n")
    print("=" * 80)

    for idx, collection in enumerate(collections, 1):
        info = extract_random_granule_info(collection)

        print(f"\nCollection {idx}:")
        print(f"  Concept ID: {info['concept_id']}")
        print(f"  Collection File Formats: {', '.join(info['collection_file_formats']) if info['collection_file_formats'] else 'None'}")
        print(f"  Granule File Formats: {', '.join(info['granule_file_formats']) if info['granule_file_formats'] else 'None'}")
        print(f"  Data Variables: {info['data_variables'] if info['data_variables'] else 'None'}")
        print(f"  Spatial Extent: {info['spatial_extent'] if info['spatial_extent'] else 'None'}")
        print(f"  Temporal Extent: {info['temporal_extent'] if info['temporal_extent'] else 'None'}")
        print(f"  Data Centers: {', '.join(info['data_centers']) if info['data_centers'] else 'None'}")
        print(f"  Data URL: {info['granule_data_url'] if info['granule_data_url'] else 'None'}")

        print("-" * 80)

auth = earthaccess.login()
if __name__ == "__main__":
    main()