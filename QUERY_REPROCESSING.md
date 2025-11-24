# Querying and Reprocessing Collections by Incompatibility Reason

This guide explains how to use S3 path-based filtering to query and reprocess collections by their tiling status and incompatibility reason.

This feature was created and utilized for troubleshooting issues with the testing code in this titiler-cmr-compatibility repo and in the TiTiler-CMR codebase.

## How It Works

Results are stored in S3 with the status and reason encoded in the path:

```
s3://bucket/collections/{concept_id}/status={true|false}/reason={reason}/result.json
```

**Examples:**
- `s3://bucket/collections/C1234/status=false/reason=unsupported_format/result.json`
- `s3://bucket/collections/C5678/status=true/reason=none/result.json`
- `s3://bucket/collections/C9012/status=false/reason=tile_generation_failed/result.json`

This structure allows efficient filtering using S3 prefix queries without reading file contents.

## Available Incompatibility Reasons

- `unsupported_format` - File format or extension not supported
- `cant_open_file` - Error opening the file
- `tile_generation_failed` - Tiling failed for unknown reason
- `no_granule_found` - No granule found in collection
- `failed_to_extract_url` - Failed to extract metadata
- `timeout` - Processing timed out
- `cant_extract_variables` - Could not extract data variables
- `group_structure` - File has group structure (hierarchical)
- `no_xy_dimensions` - Missing required x/y dimensions
- `none` - No incompatibility (tiling succeeded)

## CLI Commands

### 1. Query Collections

List collections matching specific criteria:

```bash
# Get all failed collections
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-query \
  --s3-bucket your-bucket \
  --tiling-compatible false

# Get all collections that failed for a specific reason
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-query \
  --s3-bucket your-bucket \
  --incompatibility-reason tile_generation_failed
```

### 2. Reprocess by Incompatibility Reason

Reprocess all collections that failed with a specific reason:

```bash
# Reprocess all collections that failed due to tile generation
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-reprocess-reason tile_generation_failed \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections \
  --lithops-config-file lithops.yaml
```

## Workflow Example

Here's a typical workflow for fixing issues:

1. **Query to see what failed:**

You can use the command line utility to see which collections failed.

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops --lithops-query \
  --s3-bucket my-bucket \
  --tiling-compatible false \
  --incompatibility-reason tile_generation_failed
```

However this won't give you much information about why it failed, such as the error message. In practice it makes more sense to download the results of processing using `--lithops-download` argument. The query the results of the `tiling_results.parquet` file for the error messages associated with a particular failure.

2. **Fix the underlying issue** (e.g., update code, fix configuration)

3. **Reprocess the failed collections:**

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops --lithops-reprocess-reason tile_generation_failed \
  --s3-bucket my-bucket \
  --lithops-config-file lithops.yaml
```

4. **Download all results:**

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops --lithops-download \
  --s3-bucket my-bucket \
  --output-file results.parquet
```
