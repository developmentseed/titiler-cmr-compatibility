# TiTiler-CMR Assessment Update Plan

## Goal
Rerun the compatibility assessment using the updated TiTiler-CMR API. We expect the newly added feature of NetCDF/HDF5/zarr group support to promote the status of many collections.

## Recommended approach
Use the deployed TiTiler-CMR API directly rather than packaging TiTiler-CMR into Lithops for the main assessment run.

For each collection:
1. Select one random granule from CMR
2. Persist the chosen granule identifiers for reproducibility
3. Determine the backend and request parameters
4. Use `/compatibility` to identify candidate groups when the granule has hierarchical structure
5. If groups are returned, test each candidate group and mark the collection compatible only if at least one group renders successfully
6. Run a primary render probe against `/{backend}/tiles/0/0/0` using `granule_ur` (and whatever `group` parameters were discovered in the previous step)
7. Record success/failure, response details, tested groups, and failure classification
8. Distinguish "hierarchical but still not tileable" cases from "group support missing" cases by inspecting whether the selected group opens into a dataset with usable spatial coordinates/dimensions

Use distributed deep inspection only as a fallback diagnostic workflow for cases where API-based compatibility and group enumeration are insufficient.

## Execution phases

### Phase 1: Validate probe behavior
Before full scale execution, validate the API-first method on a small sample:
- confirm `/compatibility` returns usable group lists for hierarchical files
- confirm group-by-group probing is feasible for assessment workloads
- confirm rate limiting, retries, and timeout behavior are acceptable
- confirm failure categories distinguish transient API issues from dataset issues

Deliverable:
- Finalized probe rules, group-testing rules, and retry policy

### Phase 2: Full rerun
Run the full collection assessment with the validated API-first probe.

Deliverable:
- Fresh compatibility results for all eligible collections
- Updated summary of compatibility rates and failure reasons
- Group-aware results showing which sampled granules had usable groups

## Probe design

### Primary probe
`/{backend}/tiles/0/0/0` with:
- `collection_concept_id`
- `granule_ur`
- backend-specific variable selection when needed
- group parameter when testing hierarchical files

Reason:
- directly exercises the new `granule_ur` support
- allows group-by-group testing when `/compatibility` returns hierarchical structure information

### Group-aware compatibility rule
When `/compatibility` returns groups for a sampled granule:
- test each candidate group individually
- mark the collection compatible if any group renders successfully
- record which groups succeeded and which failed
- if a group opens in xarray but lacks usable spatial coordinates/dimensions for titiler-cmr, classify it separately from missing-group support

If API-based group enumeration is unavailable or ambiguous, use a separate distributed deep-inspection workflow only for the affected subset.

### Failure classification note for non-standard hierarchical datasets
Some hierarchical HDF5 or NetCDF files may open successfully once a group is selected, but still remain unsuitable for titiler-cmr because the resulting dataset does not expose usable spatial coordinates in a form xarray/titiler-cmr can interpret. For example, a group may contain variables like `Latitude`, `Longitude`, and `Time` while the actual dimensions are named `YDim`, `XDim`, and `nTimes`, leaving xarray with "dimensions without coordinates".

These should not be counted as collections newly unlocked by group support alone. In the refreshed assessment, classify them as a separate data-structure problem, for example:
- `nonstandard_hdf5_coordinates`
- or `group_opens_but_not_georeferenced`

That keeps true titiler-cmr limitations separate from source-data modeling problems.

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
- whether the opened group exposed usable spatial coordinates/dimensions
- compatibility result
- failure reason
- raw error details

## Success criteria
- The method clearly captures newly accessible collections enabled by group support and broader DAAC bucket access
- The probe is reproducible at the granule level
- Hierarchical files are assessed by testing discovered groups rather than being reduced to a single opaque failure
- Non-standard hierarchical data-model problems are separated from titiler-cmr feature gaps
- Transient API/runtime failures are separated from true compatibility failures
- The final output can be compared at least qualitatively with the prior assessment

## PR scope
This PR should define and document the updated assessment method first. Implementation and execution can follow in subsequent commits or PRs if needed.
