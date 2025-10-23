import requests
import json
import os
import earthaccess
import random
import argparse
from typing import Optional, List, Dict, Any, Tuple
from titiler_cmr.titiler.cmr.backend import CMRBackend
from titiler_cmr.titiler.cmr.reader import xarray_open_dataset
from rio_tiler.io import Reader
from titiler.xarray.io import Reader as XarrayReader

from helpers import open_xarray_dataset, open_rasterio_dataset
from umm_helpers import parse_temporal, parse_bounds_from_spatial

titiler_cmr_endpoint = "https://staging.openveda.cloud/api/titiler-cmr"
x, y, z = 0, 0, 0
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


def fetch_granule_metadata(concept_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the first granule metadata for a given collection concept ID.

    Args:
        concept_id: Collection concept ID

    Returns:
        First granule metadata dictionary or None if no granules found
    """
    url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    params = {
        "collection_concept_id": concept_id,
        "page_size": 1
    }

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        total_num_granules = response.json().get("hits")
        params["offset"] = random.randint(0, min(total_num_granules, int(1e6)))
        response = requests.get(url, params=params, headers=headers, timeout=30)
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
    url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    params = {
        "concept_id": granule_id,
        "page_size": 1
    }

    headers = {
        "Accept": "application/vnd.nasa.cmr.umm_results+json"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        granules = data.get("items", [])

        if granules:
            return granules[0]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching granule metadata for granule {granule_id}: {e}")
        return None

hdf_formats = ["HDF", "HDF5", "HDF-EOS5"]
hdf_extensions = ["hdf", "hdf5", "h5"]
netcdf_formats = ["NetCDF", "netCDF-4", "netCDFnetCDF-4 classic"]
netcdf_extensions = ["nc", "nc4"]
cog_formats = ["COG"]
cog_extensions = ["cog", "tif", "tiff"]
zarr_formats = ["Zarr"]
zarr_extensions = ["zarr"]
supported_formats = hdf_formats + netcdf_formats + cog_formats + zarr_formats + hdf_extensions + netcdf_extensions + cog_extensions + zarr_extensions

def extract_file_format_from_granule(granule: Dict[str, Any]) -> Optional[str]:
    """
    Extract file format from granule metadata.
    First checks DataGranule/ArchiveAndDistributionInformation/Format,
    then falls back to URL suffix analysis.

    Args:
        granule: Granule metadata dictionary

    Returns:
        File format string or None if not found
    """
    umm = granule.get("umm", {})

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

    # Check both the format name and extension mappings
    format_upper = file_format.upper()

    # Direct format name matches
    if format_upper in [fmt.upper() for fmt in supported_formats]:
        return True

    # Extension to format mappings
    extension_mappings = {
        'NC': ['NetCDF', 'netCDF-4', 'netCDFnetCDF-4 classic'],
        'NC4': ['NetCDF', 'netCDF-4', 'netCDFnetCDF-4 classic'],
        'H5': ['HDF', 'HDF5', 'HDF-EOS5'],
        'HDF': ['HDF', 'HDF5', 'HDF-EOS5'],
        'HDF5': ['HDF', 'HDF5', 'HDF-EOS5'],
        'TIF': ['COG'],
        'TIFF': ['COG'],
        'COG': ['COG'],
        'ZARR': ['Zarr']
    }

    # Check if the format is a supported extension
    if format_upper in extension_mappings:
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

def generate_tiles_url_for_granule(granule_id: str) -> Optional[str]:
    """
    Generate a tiles URL for a specific granule.

    Args:
        granule_id: Granule concept ID

    Returns:
        Tiles URL string or None if unable to generate
    """
    print(f"Fetching granule metadata for granule ID: {granule_id}")

    # Fetch granule metadata
    granule = fetch_granule_by_id(granule_id)
    if not granule:
        print(f"Could not find granule with ID: {granule_id}")
        return None

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

    granule_format = extract_file_format_from_granule(granule)
    if not granule_format:
        granule_format = os.path.splitext(granule_data_url)[1].lstrip('.')

    print(f"Granule format: {granule_format}")
    print(f"Data URL: {granule_data_url}")

    # Check if format is supported
    if not is_supported_format(granule_format):
        print(f"Format {granule_format} is not supported")
        return None

    # Get data center name
    data_center_name = granule_meta["provider-id"]

    # Extract data variables
    backend, data_variables = extract_data_variables(granule_data_url, granule_format, data_center_name)
    if not backend or not data_variables:
        print("Could not extract data variables")
        return None

    print(f"Backend: {backend}")
    print(f"Data variables: {data_variables}")

    # Extract spatial and temporal extent
    spatial_extent = parse_bounds_from_spatial(granule_umm)
    temporal_extent = parse_temporal(granule_umm)

    print(f"Spatial extent: {spatial_extent}")
    print(f"Temporal extent: {temporal_extent}")

    # Generate tiles URL
    tiles_url = None
    cmr_query = {
        "concept_id": collection_concept_id,
        "temporal": temporal_extent,
    }
    shared_args = {
        "tile_x": x,
        "tile_y": y,
        "tile_z": z,
        "cmr_query": cmr_query
    }
    reader_options = {}

    if backend == "rasterio":
        band = next((item for item in data_variables if item in known_bands), None)
        if band:
            tiles_url = f"{titiler_cmr_endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png?concept_id={collection_concept_id}&backend={backend}&bands={band}"
            print(f"Using band: {band}")
            shared_args["bands_regex"] = ".*"
        else:
            print("No known band found for rasterio backend")
    elif backend == "xarray":
        variable = next((item for item in data_variables if item in known_variables), None)
        if variable:
            tiles_url = f"{titiler_cmr_endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png?concept_id={collection_concept_id}&backend={backend}&variable={variable}&datetime={('/').join(temporal_extent)}"
            print(f"Using variable: {variable}")
        else:
            print("No known variable found for xarray backend")

    if tiles_url:
        print(f"\nGenerated Tiles URL: {tiles_url}")

        # Test the tile generation
        try:
            if backend == "rasterio":
                reader = Reader
            else:  # xarray
                reader = XarrayReader
                reader_options = {
                    "variable": variable,
                    "opener": xarray_open_dataset
                }

            with CMRBackend(
                reader=reader,
                auth=auth,
                reader_options=reader_options,
            ) as src_dst:
                image, _ = src_dst.tile(**shared_args)
            print("✓ Successfully tested tile generation")
        except Exception as e:
            print(f"✗ Error testing tile generation: {e}")
            raise e
            # return None

    return tiles_url


def extract_collection_info(collection: Dict[str, Any]) -> Dict[str, Any]:
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
    collection_file_formats = []
    archive_info = umm.get("ArchiveAndDistributionInformation", {})
    file_archive_info = archive_info.get("FileArchiveInformation", None)

    if file_archive_info:
        if isinstance(file_archive_info, dict):
            collection_file_formats.append(file_archive_info.get("Format", "Unknown"))
        ## TODO: handle cases where there are multiple file formats
        elif isinstance(file_archive_info, list):
            for archive in file_archive_info:
                collection_file_formats.append(archive.get("Format", "Unknown"))

    # Extract file format from granule metadata and data variables if supported
    granule_file_formats = []
    data_variables = []
    backend = None
    spatial_extent = None
    temporal_extent = None
    granule_data_url = None

    # Extract data center short name
    data_centers = umm.get("DataCenters", [])
    data_center_names = []

    for dc in data_centers:
        short_name = dc.get("ShortName", "Unknown")
        if short_name not in data_center_names:
            data_center_names.append(short_name)

    granule = fetch_granule_metadata(concept_id)
    if granule:
        print(f"Granule concept id is {granule["meta"]["concept-id"]}")
        # get granule file format
        granule_data_url = get_data_url(granule)
        granule_format = extract_file_format_from_granule(granule)
        if granule_format:
            granule_file_formats = [granule_format]
        else:
            granule_file_formats = [os.path.splitext(granule_data_url)[1].lstrip('.')]

        # TODO(low): how to handle cases where there are multiple file formats?
        granule_file_format = granule_file_formats[0]

        # if granule file format is supported, extract data variables, spatial and temporal extent
        # also check that if collection file formats is populated, that file format is supported (e.g. HDF-EOS2 is not supported)
        if (granule_data_url and is_supported_format(granule_file_format)) and (collection_file_formats == [] or is_supported_format(collection_file_formats[0])):
            backend, data_variables = extract_data_variables(granule_data_url, granule_file_format, data_center_names[0])
            granule_umm = granule.get("umm", {})
            spatial_extent = parse_bounds_from_spatial(granule_umm)
            temporal_extent = parse_temporal(granule_umm)

    # Extract direct distribution information regions
    regions = []
    direct_dist_info = umm.get("DirectDistributionInformation")

    if direct_dist_info:
        if isinstance(direct_dist_info, dict):
            region = direct_dist_info.get("Region", "Unknown")
            regions.append(region)
        elif isinstance(direct_dist_info, list):
            for dist_info in direct_dist_info:
                region = dist_info.get("Region", "Unknown")
                if region not in regions:
                    regions.append(region)

    tiles_url = None
    if backend and data_variables:
        query = {
            "concept_id": concept_id,
            "temporal": temporal_extent,
        }
        shared_args = {
            "tile_x": x,
            "tile_y": y,
            "tile_z": z,
            "cmr_query": query,
        }
        reader_options = {}

        if backend == "rasterio":
            band = next((item for item in data_variables if item in known_bands), None)
            if band:
                tiles_url = f"{titiler_cmr_endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png?concept_id={concept_id}&backend={backend}&bands={band}"
                reader = Reader
                shared_args["bands_regex"] = ".*"
        elif backend == "xarray":
            variable = next((item for item in data_variables if item in known_variables), None)
            if variable:
                tiles_url = f"{titiler_cmr_endpoint}/tiles/WebMercatorQuad/{z}/{x}/{y}.png?concept_id={concept_id}&backend={backend}&variable={variable}&datetime={('/').join(temporal_extent)}"
                reader = XarrayReader
                reader_options = {
                    "variable": variable,
                    "opener": xarray_open_dataset
                }                


        if tiles_url:
            print(f"Tiles URL: {tiles_url}")

            try:
                with CMRBackend(
                    reader=reader,
                    auth=auth,
                    reader_options=reader_options,
                ) as src_dst:
                    image, _ = src_dst.tile(**shared_args)

                    # TODO(low): add ability to render image with colormapping
                    # png_bytes = image.render(img_format="png")
                    # with open("output.png", "wb") as f: f.write(png_bytes); f.close()
                print(f"Successfully created tile for file {granule_data_url}")
            except Exception as e:
                print(f"Error creating tile for file {granule_data_url}: {e}")
                raise e        
        else:
            print("No tiles URL could be constructed")

    return {
        "concept_id": concept_id,
        "collection_file_formats": collection_file_formats,
        "granule_file_formats": granule_file_formats,
        "granule_data_url": granule_data_url,
        "data_variables": data_variables,
        "spatial_extent": spatial_extent,
        "temporal_extent": temporal_extent,
        "data_centers": data_center_names,
        "direct_distribution_regions": regions if regions else None
    }


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
        tiles_url = generate_tiles_url_for_granule(args.granule_id)
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
        info = extract_collection_info(collection)

        print(f"\nCollection {idx}:")
        print(f"  Concept ID: {info['concept_id']}")
        print(f"  Collection File Formats: {', '.join(info['collection_file_formats']) if info['collection_file_formats'] else 'None'}")
        print(f"  Granule File Formats: {', '.join(info['granule_file_formats']) if info['granule_file_formats'] else 'None'}")
        print(f"  Data Variables: {info['data_variables'] if info['data_variables'] else 'None'}")
        print(f"  Spatial Extent: {info['spatial_extent'] if info['spatial_extent'] else 'None'}")
        print(f"  Temporal Extent: {info['temporal_extent'] if info['temporal_extent'] else 'None'}")
        print(f"  Data Centers: {', '.join(info['data_centers']) if info['data_centers'] else 'None'}")
        print(f"  Data URL: {info['granule_data_url'] if info['granule_data_url'] else 'None'}")

        if info['direct_distribution_regions']:
            print(f"  Direct Distribution Regions: {', '.join(info['direct_distribution_regions'])}")
        else:
            print(f"  Direct Distribution Regions: Not present in metadata")

        print("-" * 80)

auth = earthaccess.login()
if __name__ == "__main__":
    main()