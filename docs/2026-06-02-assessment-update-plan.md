# TiTiler-CMR Assessment Update Plan

## Goal
Rerun the compatibility assessment using the updated TiTiler-CMR behavior, with emphasis on datasets that were previously inaccessible but are now probably accessible thanks to new features in titiler-cmr:
- NetCDF/HDF5 group support
- EDL authentication

## Recommended approach
Use the deployed TiTiler-CMR API directly rather than packaging TiTiler-CMR into Lithops for the main assessment run.

For each collection:
1. Select one random granule from CMR
2. Persist the chosen granule identifiers for reproducibility
3. Determine the backend and request parameters
4. Use `/compatibility` to identify candidate groups when the granule has hierarchical structure
5. Run a primary render probe against `/{backend}/bbox` using `granule_ur`
6. If groups are returned, test each candidate group and mark the collection compatible if any group renders successfully
7. Record success/failure, response details, tested groups, and failure classification

Use `point/{lon},{lat}` only as a secondary diagnostic probe when needed, not as the primary compatibility test.

Use distributed deep inspection only as a fallback diagnostic workflow for cases where API-based compatibility and group enumeration are insufficient.

## Execution phases

### Phase 1: Targeted rerun
Rerun collections from the previous assessment most likely affected by the TiTiler-CMR changes:
- `GROUP_STRUCTURE`
- `FORBIDDEN`
- selected `CANT_OPEN_FILE` cases that may have been access-related

Deliverable:
- Quick comparison of how many previously failing collections now succeed
- Notes on any new recurring failure modes

### Phase 2: Validate probe behavior
Before full scale execution, validate the API-first method on a small sample:
- confirm `bbox + granule_ur` works across both rasterio and xarray backends
- confirm `/compatibility` returns usable group lists for hierarchical files
- confirm group-by-group probing is feasible for assessment workloads
- confirm rate limiting, retries, and timeout behavior are acceptable
- confirm failure categories distinguish transient API issues from dataset issues

Deliverable:
- Finalized probe rules, group-testing rules, and retry policy

### Phase 3: Full rerun
Run the full collection assessment with the validated API-first probe.

Deliverable:
- Fresh compatibility results for all eligible collections
- Updated summary of compatibility rates and failure reasons
- Group-aware results showing which sampled granules had usable groups

## Probe design

### Primary probe
`/{backend}/bbox` with:
- `concept_id`
- `granule_ur`
- derived bbox for the selected granule
- backend-specific variable selection when needed
- group parameter when testing hierarchical files

Reason:
- directly exercises the new `granule_ur` support
- better represents “can this granule be rendered?” than a global `z=0/x=0/y=0` tile
- allows group-by-group testing when `/compatibility` returns hierarchical structure information

### Secondary diagnostic probe
`/{backend}/point/{lon},{lat}` for a point inside the granule geometry.

Reason:
- useful for troubleshooting
- too easy to produce false negatives to serve as the main verdict

### Group-aware compatibility rule
When `/compatibility` returns groups for a sampled granule:
- test each candidate group individually
- mark the collection compatible if any group renders successfully
- record which groups succeeded and which failed

If API-based group enumeration is unavailable or ambiguous, use a separate distributed deep-inspection workflow only for the affected subset.

## Data to persist per collection
- collection concept ID
- selected granule concept ID
- selected granule UR
- backend
- probe URL or normalized request parameters
- selected variable, if applicable
- groups returned by `/compatibility`
- groups tested
- groups that succeeded
- groups that failed
- compatibility result
- failure reason
- raw error details

## Success criteria
- The method clearly captures newly accessible collections enabled by group support and broader DAAC bucket access
- The probe is reproducible at the granule level
- Hierarchical files are assessed by testing discovered groups rather than being reduced to a single opaque failure
- Transient API/runtime failures are separated from true compatibility failures
- The final output can be compared at least qualitatively with the prior assessment

## PR scope
This PR should define and document the updated assessment method first. Implementation and execution can follow in subsequent commits or PRs if needed.
