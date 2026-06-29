# Methodology: TiTiler-CMR Compatibility Assessment

## Goal

The goal of this repository is to assess the compatibility of collections in NASA's CMR with [TiTiler-CMR](https://github.com/developmentseed/titiler-cmr/). The assessment determines whether collections can be successfully tiled using the TiTiler-CMR backend, which enables visualization of geospatial data through web map tile generation.

## Overview

This methodology document describes the approach used to evaluate around 10,000 of NASA Earth data collections for their ability to generate tiles via TiTiler-CMR. The assessment tests each collection by:

1. Fetching collection metadata from NASA CMR
2. Selecting a random granule from each collection
3. Extracting file format and variable information
4. Attempting to generate a tile
5. Recording the outcome: whether the tiling operation was successful (`tiling_compatible=True`) or unsuccessful (`tiling_compatible=False` along with recording an error message and one of a predefined set of incompatibility reasons).

## Collection Processing Workflow

### Phase 1: Collection Discovery

The system queries NASA's CMR API to discover collections:

- **Endpoint**: `https://cmr.earthdata.nasa.gov/search/collections.umm_json`
- **Parameters**:
  - `has_granules=True`
  - `providers[]=<LIST OF EOSDIS PROVIDERS>`: `get_eosdis_providers.py` provides a filtered list of providers associated with EOSDIS. This filters the collections to those associated with the NASA DAACs.
- **Processing modes**:
  - Testing and troublshooting: Process collections or granules one at a time
  - Parallel: Process multiple collections using multiprocessing
  - Lithops: Distributed processing using AWS Lambda

### Phase 2: Granule Selection

For each collection, a random granule is selected to test:

1. **Random granule fetch**: Fetch a random granule from the collection by using a random offset in the range of 0 to the total number of granules in that collection.
2. **Metadata extraction**: Extract granule-level metadata including:
   - Concept ID (unique identifier)
   - Data URL (direct S3 link or external HTTP link)
   - Temporal extent 
   - File format and extension
   - Data center/DAAC information

**Key code location**: `metadata.py#extract_random_granule_info`)

### Phase 3: Format Validation

The system validates whether the granule's format is supported:

#### Supported Formats

- **Cloud-Optimized GeoTIFF (COG)**: `COG`, extensions: `.cog`, `.tif`, `.tiff`
- **NetCDF**: `NetCDF`, `netCDF-4`, `netCDF-4 classic`, extensions: `.nc`, `.nc4`
- **HDF**: `HDF`, `HDF5`, `HDF-EOS5`, extensions: `.hdf`, `.hdf5`, `.h5`
- **Zarr**: `zarr`, extension: `.zarr`

**Key code location**: `constants.py`

If the format is unsupported, the granule is marked with:
- `incompatible_reason`: `UNSUPPORTED_FORMAT`
- `tiling_compatible`: `false`

### Phase 4: Data Access and Variable Extraction

The system attempts to open the granule and extract data variables:

#### Backend Selection

Two backends are used depending on file format:

1. **Rasterio backend** (for COG/GeoTIFF formats):
   - Opens file using `rasterio`
   - Extracts band descriptions as variables, however no additional variable parameter is needed for tiling

2. **Xarray backend** (for NetCDF/HDF formats):
   - Opens file using `xarray` with `h5netcdf` engine
   - Extracts data variables from the dataset
   - Requires explicit variable selection for tiling
   - Prioritizes "known variables" (common science variables)

**Key code location**: `tiling.py#_extract_data_variables_and_backend`)

#### Error Handling

When opening data, multiple error types were encountered. These error types were used to define various `IncompatibilityReason`s.

| Error Pattern | Incompatibility Reason | Description |
|--------------|----------------------|-------------|
| "not the signature of a valid netCDF4 file" | `UNSUPPORTED_FORMAT` | Invalid or corrupted NetCDF file |
| "Cannot seek streaming HTTP file" | `UNSUPPORTED_FORMAT` | File requires byte-range access |
| "Forbidden" or "Unauthorized" | `FORBIDDEN` | Access permission denied |
| "Operation timed out" | `TIMEOUT` | Network timeout during access |
| "not aligned with its parents" | `GROUP_NOT_ALIGNED_WITH_PARENTS` | NetCDF group structure issue |
| "can only convert an array of size 1" | `DECODE_ERROR` | Data decoding failure |
| Other errors | `CANT_OPEN_FILE` | General file access failure |

#### Group Structure Detection

If no variables are extracted, the system checks for NetCDF/HDF group structures. Groups indicate hierarchical organization within the file.
If groups are found they are marked with `incompatible_reason=GROUP_STRUCTURE`. Datasets with groups are considered for future support.

**Key code location**: `tiling.py`, look for `check_for_groups`.

### Phase 5: Tile Generation Test

The system attempts to generate a test tile using the TiTiler-CMR backend:

#### Test Parameters

- **Tile coordinates**: 0-0-0 (zoom level 0, tile x=0, y=0)
- **Endpoint**: `https://staging.openveda.cloud/api/titiler-cmr`

#### Tiling Process

1. **CMR query construction**: Create query with collection concept ID and temporal extent
2. **Reader initialization**: Set up rasterio or xarray reader with appropriate options
3. **Compatibility probe**: Call `/compatibility`, retrying transient 5xx service failures before recording a failure
4. **CMR backend execution**: Use `CMRBackend` to fetch granule and generate tile
5. **BBox rendering**: Render the full granule CMR bounding box first. If the request is rejected because the AOI/array is too large, or if a large xarray grid returns a transient full-bbox service failure, retry deterministic smaller bboxes within the granule extent.

**Key code location**: `tiling.py#test_tiling`

#### Success Criteria

A collection is marked as **tiling compatible** (`tiling_compatible: true`) only when the TiTiler-CMR
`/bbox` render probe returns HTTP 200. HTTP 204 responses are recorded as inconclusive
`no_rendered_content` failures because the endpoint returned no image content.

For xarray assets, the bbox probe tries the variable recommended by the `/compatibility` response link
first, then a bounded ranked fallback list. Non-spatial singleton dimensions are not forced through
`sel` because TiTiler-CMR can render those directly. Each variable is first tested against the full
granule bbox. Smaller deterministic bboxes are only attempted when the service reports that the full AOI/array is too
large, or when a large xarray grid returns a transient full-bbox service failure. The run records every bbox
probe attempt, including status code, variable source, group, probe bbox, URL, and an error snippet.

#### Failure Modes

| Incompatibility Reason | Description |
|-----------------------|-------------|
| `NO_GRANULE_FOUND` | No granules available for the collection |
| `FAILED_TO_EXTRACT_URL` | Unable to extract data URL from metadata |
| `UNSUPPORTED_FORMAT` | File format not supported by TiTiler |
| `CANT_OPEN_FILE` | File cannot be opened with rasterio/xarray |
| `CANT_EXTRACT_VARIABLES` | No data variables found in file |
| `GROUP_STRUCTURE` | File has group structure requiring special handling |
| `NO_XY_DIMENSIONS` | Xarray dataset missing required X/Y dimensions |
| `GROUP_NOT_ALIGNED_WITH_PARENTS` | NetCDF group alignment issue |
| `DECODE_ERROR` | Error decoding data values |
| `FORBIDDEN` | Access denied to the data |
| `TIMEOUT` | Operation timed out |
| `TILE_GENERATION_FAILED` | Tile generation failed for other reasons |

**Key code location**: `tiling.py#IncompatibilityReason`)

### Phase 6: Results Recording

For each collection tested, the system records:

- **Collection metadata**:
  - `collection_concept_id`: Unique collection identifier
  - `collection_short_name_and_version`: Human-readable collection name
  - `num_granules`: Total granules in collection
  - `processing_level`: Science processing level (e.g., L1, L2, L3)
  - `data_center`: Responsible DAAC (e.g., NSIDC, PODAAC)

- **Granule metadata**:
  - `concept_id`: Tested granule identifier
  - `data_url`: S3 or HTTP URL for the data file

- **Format information**:
  - `format`: File format from metadata
  - `extension`: Legacy file extension from the first granule related URL
  - `assessed_asset_href`, `assessed_asset_extension`, and `assessed_asset_scheme`: Asset selected by the
    TiTiler-CMR compatibility response
  - `backend`: Backend used (`rasterio` or `xarray`)

- **Variables**:
  - `data_variables`: List of available variables
  - `variable`: Selected variable for tiling (xarray only)
  - `selected_variable_source`: Whether the selected variable came from the compatibility link, ranked
    fallback, capped fallback list, or rasterio no-variable path

- **Compatibility results**:
  - `tiling_compatible`: Boolean success/failure retained for the compatibility report table
  - `incompatible_reason`: Legacy categorized failure reason (if applicable)
  - `error_message`: Human-readable failure summary
  - `assessment_status`: High-level outcome (`compatible`, `incompatible`, `inconclusive`, or `assessment_error`)
  - `failure_stage`: Assessment step that failed, such as `sampling`, `compatibility`, `bbox_resolution`, or `bbox_probe`
  - `failure_category` and `failure_subcategory`: Machine-readable failure classification for distinguishing
    unsupported data from metadata gaps and service/runtime failures
  - `failure_http_status_code`, `failure_endpoint`, `failure_url`, and `raw_error_body`: Request-level
    diagnostics for failed TiTiler-CMR probes
  - `bbox_attempt_count` and `bbox_attempts`: Serialized attempt history for bounded xarray variable
    probing and multi-bbox global/swath probing, including full request URLs with query parameters
  - `tiles_url`: Generated tile URL (if successful), including query parameters
  - `groups`: List of detected groups (if applicable)

**Key code location**: `tiling.py#to_report_dict`)

## Processing Modes

### Sequential Processing

Basic mode for testing individual or small numbers of collections or granules:

```bash
python run_test.py --collection-id <CONCEPT_ID>
python run_test.py --granule-id <CONCEPT_ID>
python run_test.py --page-size 10
```

### Parallel Processing

Uses Python multiprocessing for faster batch processing:

```bash
python run_test.py --parallel --num-workers 8 --total-collections 1000
```

- Processes collections in configurable batch sizes (default: 25)
- Configurable timeout per collection (default: 30s)
- Results saved incrementally to Parquet file

### Distributed Processing (Lithops)

See [LITHOPS_WORKFLOW.md](./LITHOPS_WORKFLOW.md)

