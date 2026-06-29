# TiTiler-CMR Compatibility

This repository contains code for testing tile generation for NASA Earth data collections using [TiTiler-CMR](https://github.com/developmentseed/titiler-cmr).

## What's here?

* [METHODOLOGY.md](./METHODOLOGY.md): The methodology documentation explains the testing steps.
* [LITHOPS_WORKFLOW.md](./LITHOPS_WORKFLOW.md): The lithops workflow documentation how to setup lithops and use lithops parallel processing.
* [QUERY_REPROCESSING.md](./QUERY_REPROCESSING.md): The query reprocessing documentation explains how to use the CLI to reprocess any failed collections.
* [titiler_cmr/](./titiler_cmr/): TiTiler-CMR installed as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules).
* [titiler_cmr_compatibility/](./titiler_cmr_compatibility/): Various modules used to query CMR and test tile generation.
* [read_results.ipynb](./read_results.ipynb): Basic notebook for inspecting tiling results. The full report on results is deferred to documentation in [TiTiler-CMR's docs](https://developmentseed.org/titiler-cmr/dev/).

## How to use it

First clone the repo:

```bash
git clone https://github.com/developmentseed/titiler-cmr-compatibility.git
cd titiler-cmr-compatibility/
```

Instead of calling the TiTiler-CMR API directly, the library is installed as a git submodule. This gets around any networking limitations and slow downs of using the actual API and also allows to make small fixes to the titiler-cmr codebase (which will be addressed in near-future TiTiler-CMR releases).

```bash
git submodule update --init --recursive
pip install -e ./titiler_cmr
uv sync
```

You can assess one sampled granule through the deployed TiTiler-CMR API with:

```python
import asyncio

from titiler_cmr_compatibility.assessment import assess_collection_compatibility

result = asyncio.run(
    assess_collection_compatibility(
        collection_concept_id="C1234567890-PROVIDER",
        granule_ur="sample-granule-ur",
    )
)
print(result["compatible"])
```

You can assess one collection from the CLI. The command samples a granule for the collection, derives a small subset of that granule's CMR bounding box, calls the TiTiler-CMR compatibility and `/bbox` APIs, and prints the result as JSON.

```bash
uv run titiler-cmr-assessment assess-collection C1234567890-PROVIDER
```

You can assess a small batch of eligible CMR collections and write a parquet artifact with:

```bash
uv run titiler-cmr-assessment assess-collections \
  --limit 5 \
  --output assessment-validation.parquet
```

For a full run, omit `--limit` and choose a dated output path. Full runs call both CMR and the deployed TiTiler-CMR API many times, so keep concurrency conservative unless you have validated higher values for the target endpoint. In an interactive terminal, the batch command shows a tqdm progress bar with compatible, incompatible, error, and in-flight details stacked below it. Use `--no-progress` to keep output to coarse phase messages only.

```bash
uv run titiler-cmr-assessment assess-collections \
  --output assessment-results-YYYY-MM-DD.parquet \
  --concurrency 5 \
  --max-bbox-variable-attempts 10
```

To reprocess an explicit subset, pass repeated collection concept IDs or read newline-delimited IDs from a file. Use `-` to read from stdin, which makes DuckDB result pipes convenient:

```bash
duckdb -noheader -list -c "
select distinct collection_concept_id
from read_parquet('assessment-results-2026-07-01.parquet')
where error_code = 'bbox_probe_attempt_limit_exceeded';
" | uv run titiler-cmr-assessment assess-collections \
  --collection-concept-ids-file - \
  --output assessment-reprocessed.parquet
```

The batch parquet preserves the legacy columns consumed by `../titiler-cmr/docs/compatibility/`,
including `collection_concept_id`, `collection_short_name_and_version`, `concept_id`, `data_center`,
`data_url`, `backend`, `format`, `extension`, `tiling_compatible`, `incompatible_reason`,
`error_message`, `tiles_url`, `variable`, `data_variables`, `num_granules`, `processing_level`, and
`groups`. It also includes additive diagnostic columns (`assessment_status`, `failure_stage`,
`failure_category`,
`failure_subcategory`, `error_code`, `error_detail`, `failure_http_status_code`, `failure_endpoint`,
`failure_url`, and `raw_error_body`) so reports can distinguish dataset incompatibility from
inconclusive service, metadata, or probe failures. Audit columns capture the sampled granule, assessed
asset href/extension/scheme from `compatibility_response.example_assets`, backend, group and variable
selection, dimension selectors, full bbox probe attempt history, full bbox probe URLs with query parameters, and raw compatibility response details.
The assessment retries transient `/compatibility` 5xx failures. For xarray datasets with non-spatial
dimensions such as `time`, bbox probes add nearest-neighbor `sel` selectors using the sampled granule's
temporal extent to pluck a tileable slice, but singleton dimensions are left to TiTiler-CMR's native
handling. Bbox probes try the full granule CMR bbox first and back off
to smaller deterministic bboxes when the service reports that the AOI/array is too large. A `/bbox` HTTP
204 is treated as no rendered content,
not as tiling compatible.


You can use the help argument to see the CLI options.

```bash
uv run titiler-cmr-assessment --help
uv run titiler-cmr-assessment assess-collections --help
```

Next, head to [`METHODOLOGY.md`](./METHODOLOGY.md) learn more about the approach or [`LITHOPS_WORKFLOW.md`](./LITHOPS_WORKFLOW.md) to see how to setup parallel processing using [lithops](https://lithops-cloud.github.io/).



