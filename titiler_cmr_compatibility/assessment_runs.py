"""Batch assessment orchestration and parquet output helpers."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from httpx2 import AsyncClient

from .api import (
    fetch_cmr_collections_by_concept_ids,
    fetch_eligible_cmr_collections,
    fetch_random_granule_metadata,
)
from .assessment import (
    DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS,
    DEV_TITILER_CMR_ENDPOINT,
    BBox,
    assess_collection_compatibility,
)

try:
    from .metadata import (
        extract_collection_file_format,
        extract_data_center,
        extract_processing_level,
    )
except ModuleNotFoundError as exc:
    if exc.name != "titiler":
        raise
    extract_collection_file_format = None
    extract_data_center = None
    extract_processing_level = None

from .umm_helpers import parse_bounds_from_spatial, parse_temporal

logger = logging.getLogger(__name__)

DEFAULT_COLLECTION_PAGE_SIZE = 100
DEFAULT_CONCURRENCY = 10


@dataclass(frozen=True)
class AssessmentProgressEvent:
    """Structured progress update emitted during a batch assessment run."""

    name: str
    collection_concept_id: str | None = None
    total: int | None = None
    completed: int | None = None
    compatible: int | None = None
    incompatible: int | None = None
    errors: int | None = None
    in_flight: tuple[str, ...] = ()
    tiling_compatible: bool | None = None
    incompatible_reason: str | None = None
    error: bool | None = None


AssessmentProgressCallback = Callable[[AssessmentProgressEvent], None]

LEGACY_COLUMNS = [
    "collection_concept_id",
    "collection_short_name_and_version",
    "concept_id",
    "data_center",
    "data_url",
    "backend",
    "format",
    "extension",
    "tiling_compatible",
    "incompatible_reason",
    "error_message",
    "tiles_url",
    "variable",
    "data_variables",
    "num_granules",
    "processing_level",
    "groups",
]

DIAGNOSTIC_COLUMNS = [
    "assessment_status",
    "failure_stage",
    "failure_category",
    "failure_subcategory",
    "error_code",
    "error_detail",
    "failure_http_status_code",
    "failure_endpoint",
    "failure_url",
    "raw_error_body",
]

DETAIL_COLUMNS = [
    "granule_concept_id",
    "granule_ur",
    "assessed_asset_href",
    "assessed_asset_extension",
    "assessed_asset_scheme",
    "group",
    "variables",
    "selected_variable_source",
    "dimension_selectors",
    "temporal",
    "selected_dimension_source",
    "compatible_groups",
    "tested_groups",
    "granule_bbox",
    "probe_bbox",
    "bbox_status_code",
    "bbox_error_body",
    "bbox_error_snippet",
    "bbox_url",
    "bbox_attempt_count",
    "bbox_attempts",
    "bbox_probe_limited",
    "compatibility_status_code",
    "compatibility_error_body",
    "compatibility_response",
]

OUTPUT_COLUMNS = LEGACY_COLUMNS + DIAGNOSTIC_COLUMNS + DETAIL_COLUMNS
NESTED_COLUMNS = {
    "data_variables",
    "groups",
    "variables",
    "dimension_selectors",
    "compatible_groups",
    "tested_groups",
    "granule_bbox",
    "probe_bbox",
    "bbox_attempts",
    "compatibility_response",
}


def normalize_assessment_row(
    collection: Mapping[str, Any],
    granule: Mapping[str, Any] | None,
    num_granules: int | None,
    assessment: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize one successful assessment attempt into the batch output schema.

    Args:
        collection: CMR collection metadata.
        granule: Sampled CMR granule metadata, if one was found.
        num_granules: Total granule count reported by CMR for the collection.
        assessment: Result from ``assess_collection_compatibility``.

    Returns:
        A row containing all legacy summary columns plus audit/detail columns.
    """
    compatible = bool(assessment.get("compatible"))
    failure_reason = assessment.get("failure_reason")
    compatibility_response = _mapping_or_none(assessment.get("compatibility_response"))
    error_message = (
        None
        if compatible
        else _error_message(failure_reason, assessment, compatibility_response)
    )

    return _base_row(collection, granule, num_granules, compatibility_response) | {
        "tiling_compatible": compatible,
        "incompatible_reason": None
        if compatible
        else (failure_reason or "assessment_failed"),
        "error_message": error_message,
        **_diagnostic_fields(
            compatible=compatible,
            failure_reason=failure_reason,
            error_message=error_message,
            assessment=assessment,
            compatibility_response=compatibility_response,
        ),
        "backend": assessment.get("backend"),
        "tiles_url": _tiles_url(assessment, compatibility_response),
        "variable": _selected_variable(assessment),
        "data_variables": _compatibility_variables(compatibility_response),
        "group": assessment.get("group"),
        "variables": _list_or_none(assessment.get("variables")),
        "selected_variable_source": _string_or_none(
            assessment.get("selected_variable_source")
        ),
        "dimension_selectors": _list_or_none(assessment.get("dimension_selectors")),
        "temporal": _string_or_none(assessment.get("temporal")),
        "selected_dimension_source": _string_or_none(
            assessment.get("selected_dimension_source")
        ),
        "groups": _list_or_none(assessment.get("compatible_groups")),
        "compatible_groups": _list_or_none(assessment.get("compatible_groups")),
        "tested_groups": _list_or_none(assessment.get("tested_groups")),
        "granule_bbox": _sequence_or_none(assessment.get("granule_bbox")),
        "probe_bbox": _sequence_or_none(assessment.get("probe_bbox")),
        "bbox_status_code": assessment.get("bbox_status_code"),
        "bbox_error_body": _string_or_none(assessment.get("bbox_error_body")),
        "bbox_error_snippet": _snippet_or_none(assessment.get("bbox_error_body")),
        "bbox_url": _string_or_none(assessment.get("bbox_url")),
        "bbox_attempt_count": assessment.get("bbox_attempt_count"),
        "bbox_attempts": _list_or_none(assessment.get("bbox_attempts")),
        "bbox_probe_limited": bool(assessment.get("bbox_probe_limited")),
        "compatibility_status_code": _status_code_from_compatibility(
            compatibility_response
        ),
        "compatibility_error_body": _error_body_from_compatibility(
            compatibility_response
        ),
        "compatibility_response": compatibility_response,
    }


def normalize_missing_granule_row(
    collection: Mapping[str, Any],
    granule: Mapping[str, Any] | None,
    num_granules: int | None,
    reason: str,
) -> dict[str, Any]:
    """Normalize a collection whose sampled granule could not be assessed.

    Args:
        collection: CMR collection metadata.
        granule: Sampled CMR granule metadata, if one was returned.
        num_granules: Total granule count reported by CMR for the collection.
        reason: Stable incompatible reason for the sampling failure.

    Returns:
        A failure row in the batch output schema.
    """
    return _failure_row(
        collection, granule, num_granules, reason, _sampling_error_message(reason)
    )


async def assess_collections(
    collections: Sequence[Mapping[str, Any]],
    titiler_cmr_endpoint: str = DEV_TITILER_CMR_ENDPOINT,
    timeout: int = 60,
    concurrency: int = DEFAULT_CONCURRENCY,
    progress_callback: AssessmentProgressCallback | None = None,
    max_bbox_variable_attempts: int = DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS,
) -> list[dict[str, Any]]:
    """Assess many collections concurrently with collection-local failures.

    Args:
        collections: CMR collection metadata dictionaries.
        titiler_cmr_endpoint: TiTiler-CMR endpoint to call.
        timeout: Request timeout in seconds.
        concurrency: Maximum number of in-flight collection assessments.
        progress_callback: Optional callback that receives structured progress events.
        max_bbox_variable_attempts: Maximum xarray variables to try per probe bbox.

    Returns:
        One normalized row per unique collection concept ID, in input order.

    Raises:
        ValueError: If ``concurrency`` is less than one.
    """
    if concurrency < 1:
        msg = "concurrency must be greater than 0"
        raise ValueError(msg)

    unique_collections = _deduplicate_collections(collections)
    semaphore = asyncio.Semaphore(concurrency)
    progress = _AssessmentProgressState(len(unique_collections), progress_callback)
    progress.emit("batch_started")

    async with AsyncClient(timeout=timeout) as client:
        tasks = [
            _assess_collection_with_progress(
                index,
                collection,
                semaphore,
                client,
                titiler_cmr_endpoint,
                timeout,
                progress,
                max_bbox_variable_attempts,
            )
            for index, collection in enumerate(unique_collections)
        ]
        indexed_rows = await asyncio.gather(*tasks)

    progress.emit("batch_completed")
    return [row for _, row in sorted(indexed_rows, key=lambda item: item[0])]


async def run_batch_assessment(
    output_path: str | Path,
    limit: int | None = None,
    collection_concept_ids: Sequence[str] | None = None,
    titiler_cmr_endpoint: str = DEV_TITILER_CMR_ENDPOINT,
    timeout: int = 60,
    concurrency: int = DEFAULT_CONCURRENCY,
    collection_page_size: int = DEFAULT_COLLECTION_PAGE_SIZE,
    progress: Callable[[str], None] | None = None,
    progress_callback: AssessmentProgressCallback | None = None,
    max_bbox_variable_attempts: int = DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS,
) -> list[dict[str, Any]]:
    """Discover collections, assess them, and write a parquet artifact.

    Args:
        output_path: Destination parquet file path.
        limit: Optional maximum number of eligible collections to assess.
        collection_concept_ids: Optional explicit collection concept IDs to assess
            instead of discovering collections by usage score. Cannot be combined
            with ``limit``.
        titiler_cmr_endpoint: TiTiler-CMR endpoint to call.
        timeout: Request timeout in seconds.
        concurrency: Maximum number of in-flight collection assessments.
        collection_page_size: CMR collection search page size.
        progress: Optional callback for coarse progress messages.
        progress_callback: Optional callback that receives structured progress events.
        max_bbox_variable_attempts: Maximum xarray variables to try per probe bbox.

    Returns:
        Normalized assessment rows that were written to parquet.
    """
    if collection_concept_ids is not None and limit is not None:
        msg = "limit cannot be combined with explicit collection concept IDs"
        raise ValueError(msg)

    _emit_progress(progress_callback, AssessmentProgressEvent(name="discovery_started"))
    async with AsyncClient(timeout=timeout) as client:
        if collection_concept_ids is None:
            _emit(progress, "Discovering eligible CMR collections...")
            collections = await fetch_eligible_cmr_collections(
                limit=limit,
                page_size=collection_page_size,
                client=client,
                timeout=timeout,
            )
            _emit(progress, f"Discovered {len(collections)} eligible collections.")
        else:
            _emit(
                progress,
                f"Fetching {len(collection_concept_ids)} selected CMR collections...",
            )
            collections = await fetch_cmr_collections_by_concept_ids(
                collection_concept_ids,
                client=client,
                timeout=timeout,
            )
            _emit(progress, f"Fetched {len(collections)} selected CMR collections.")

    _emit_progress(
        progress_callback,
        AssessmentProgressEvent(name="discovery_completed", total=len(collections)),
    )
    _emit(progress, f"Assessing collections with concurrency={concurrency}...")
    rows = await assess_collections(
        collections,
        titiler_cmr_endpoint=titiler_cmr_endpoint,
        timeout=timeout,
        concurrency=concurrency,
        progress_callback=progress_callback,
        max_bbox_variable_attempts=max_bbox_variable_attempts,
    )
    write_assessment_parquet(rows, output_path)
    _emit(progress, f"Wrote {len(rows)} assessment rows to {output_path}.")
    return rows


def write_assessment_parquet(
    rows: Sequence[Mapping[str, Any]], output_path: str | Path
) -> None:
    """Write normalized batch rows to a downstream-compatible parquet file.

    Nested audit columns are serialized as JSON strings so pyarrow writes a
    predictable schema while legacy display columns stay simple scalar values.

    Args:
        rows: Normalized rows from ``assess_collections``.
        output_path: Destination parquet file path.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([_serialize_row(row) for row in rows], columns=OUTPUT_COLUMNS)
    frame.to_parquet(path, index=False)


async def _assess_collection_with_progress(
    index: int,
    collection: Mapping[str, Any],
    semaphore: asyncio.Semaphore,
    client: AsyncClient,
    titiler_cmr_endpoint: str,
    timeout: int,
    progress: _AssessmentProgressState,
    max_bbox_variable_attempts: int,
) -> tuple[int, dict[str, Any]]:
    collection_concept_id = _collection_concept_id(collection)
    async with semaphore:
        progress.start(collection_concept_id)
        row = await _assess_collection(
            collection,
            client,
            titiler_cmr_endpoint,
            timeout,
            max_bbox_variable_attempts,
        )
        progress.finish(collection_concept_id, row)
        return index, row


async def _assess_collection(
    collection: Mapping[str, Any],
    client: AsyncClient,
    titiler_cmr_endpoint: str,
    timeout: int,
    max_bbox_variable_attempts: int,
) -> dict[str, Any]:
    collection_concept_id = _collection_concept_id(collection)
    granule: Mapping[str, Any] | None = None
    num_granules: int | None = None

    try:
        granule, num_granules = await fetch_random_granule_metadata(
            collection_concept_id,
            client=client,
            timeout=timeout,
        )
        if not granule:
            return normalize_missing_granule_row(
                collection, granule, num_granules, "no_granule_found"
            )

        granule_ur = _granule_ur(granule)
        if not granule_ur:
            return normalize_missing_granule_row(
                collection, granule, num_granules, "no_granule_ur"
            )

        assessment = await assess_collection_compatibility(
            collection_concept_id=collection_concept_id,
            granule_ur=granule_ur,
            titiler_cmr_endpoint=titiler_cmr_endpoint,
            client=client,
            timeout=timeout,
            granule_bbox=_granule_bbox(granule),
            granule_temporal=_granule_temporal(granule),
            max_bbox_variable_attempts=max_bbox_variable_attempts,
        )
        return normalize_assessment_row(collection, granule, num_granules, assessment)
    except Exception as exc:
        logger.debug(
            "Assessment failed for collection %s", collection_concept_id, exc_info=True
        )
        return _failure_row(
            collection, granule, num_granules, "assessment_exception", str(exc)
        )


def _base_row(
    collection: Mapping[str, Any],
    granule: Mapping[str, Any] | None,
    num_granules: int | None,
    compatibility_response: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    collection_concept_id = _collection_concept_id(collection)
    assessed_asset_href = _assessed_asset_href(compatibility_response)
    data_url = assessed_asset_href or _granule_data_url(granule)
    return {
        "collection_concept_id": collection_concept_id,
        "collection_short_name_and_version": _short_name_and_version(collection),
        "concept_id": _granule_concept_id(granule),
        "data_center": _safe_data_center(collection),
        "data_url": data_url,
        "format": _safe_collection_file_format(collection),
        "extension": _granule_extension(granule),
        "assessed_asset_href": assessed_asset_href,
        "assessed_asset_extension": _extension_from_url(assessed_asset_href),
        "assessed_asset_scheme": _scheme_from_url(assessed_asset_href),
        "tiling_compatible": False,
        "incompatible_reason": None,
        "error_message": None,
        "processing_level": _safe_processing_level(collection),
        "num_granules": num_granules,
        "assessment_status": None,
        "failure_stage": None,
        "failure_category": None,
        "failure_subcategory": None,
        "error_code": None,
        "error_detail": None,
        "failure_http_status_code": None,
        "failure_endpoint": None,
        "failure_url": None,
        "raw_error_body": None,
        "tiles_url": None,
        "variable": None,
        "data_variables": None,
        "groups": None,
        "granule_concept_id": _granule_concept_id(granule),
        "granule_ur": _granule_ur(granule),
        "backend": None,
        "group": None,
        "variables": None,
        "selected_variable_source": None,
        "dimension_selectors": None,
        "temporal": None,
        "selected_dimension_source": None,
        "compatible_groups": None,
        "tested_groups": None,
        "granule_bbox": None,
        "probe_bbox": None,
        "bbox_status_code": None,
        "bbox_error_body": None,
        "bbox_error_snippet": None,
        "bbox_url": None,
        "bbox_attempt_count": None,
        "bbox_attempts": None,
        "bbox_probe_limited": None,
        "compatibility_status_code": None,
        "compatibility_error_body": None,
        "compatibility_response": None,
    }


def _failure_row(
    collection: Mapping[str, Any],
    granule: Mapping[str, Any] | None,
    num_granules: int | None,
    reason: str,
    error_message: str,
) -> dict[str, Any]:
    row = _base_row(collection, granule, num_granules)
    row["tiling_compatible"] = False
    row["incompatible_reason"] = reason
    row["error_message"] = error_message
    row.update(
        _diagnostic_fields(
            compatible=False,
            failure_reason=reason,
            error_message=error_message,
            assessment={},
            compatibility_response=None,
        )
    )
    return row


def _deduplicate_collections(
    collections: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    seen: set[str] = set()
    unique: list[Mapping[str, Any]] = []
    for collection in collections:
        concept_id = _collection_concept_id(collection)
        if concept_id in seen:
            continue
        seen.add(concept_id)
        unique.append(collection)
    return unique


def _collection_concept_id(collection: Mapping[str, Any]) -> str:
    concept_id = collection.get("meta", {}).get("concept-id")
    if not isinstance(concept_id, str) or not concept_id:
        msg = "collection metadata is missing meta.concept-id"
        raise ValueError(msg)
    return concept_id


def _short_name_and_version(collection: Mapping[str, Any]) -> str | None:
    umm = collection.get("umm", {})
    short_name = umm.get("ShortName")
    version = umm.get("Version")
    if short_name and version:
        return f"{short_name}.{version}"
    if short_name:
        return str(short_name)
    return None


def _safe_collection_file_format(collection: Mapping[str, Any]) -> str | None:
    if extract_collection_file_format is not None:
        return extract_collection_file_format(dict(collection))

    archive_info = collection.get("umm", {}).get(
        "ArchiveAndDistributionInformation", {}
    )
    file_archive_info = (
        archive_info.get("FileArchiveInformation")
        if isinstance(archive_info, Mapping)
        else None
    )
    if isinstance(file_archive_info, Mapping):
        file_format = file_archive_info.get("Format")
        return str(file_format) if file_format else None
    if isinstance(file_archive_info, list) and file_archive_info:
        file_format = (
            file_archive_info[0].get("Format")
            if isinstance(file_archive_info[0], Mapping)
            else None
        )
        return str(file_format) if file_format else None
    return None


def _safe_data_center(collection: Mapping[str, Any]) -> str | None:
    if extract_data_center is not None:
        try:
            return extract_data_center(dict(collection))
        except (IndexError, AttributeError, TypeError):
            return None

    data_centers = collection.get("umm", {}).get("DataCenters", [])
    if isinstance(data_centers, list) and data_centers:
        short_name = (
            data_centers[0].get("ShortName")
            if isinstance(data_centers[0], Mapping)
            else None
        )
        return str(short_name) if short_name else None
    return None


def _safe_processing_level(collection: Mapping[str, Any]) -> str | None:
    if extract_processing_level is not None:
        try:
            return extract_processing_level(dict(collection))
        except (AttributeError, TypeError):
            return None

    processing_level = collection.get("umm", {}).get("ProcessingLevel", {})
    if isinstance(processing_level, Mapping):
        processing_level_id = processing_level.get("Id")
        return str(processing_level_id) if processing_level_id else None
    return None


def _granule_concept_id(granule: Mapping[str, Any] | None) -> str | None:
    if not granule:
        return None
    concept_id = granule.get("meta", {}).get("concept-id")
    return concept_id if isinstance(concept_id, str) else None


def _granule_ur(granule: Mapping[str, Any] | None) -> str | None:
    if not granule:
        return None
    granule_ur = granule.get("umm", {}).get("GranuleUR")
    return granule_ur if isinstance(granule_ur, str) and granule_ur else None


def _granule_bbox(granule: Mapping[str, Any]) -> BBox | None:
    bbox = parse_bounds_from_spatial(granule.get("umm", {}))
    if not bbox:
        return None
    return tuple(bbox)  # type: ignore[return-value]


def _granule_temporal(granule: Mapping[str, Any]) -> str | None:
    begin, end = parse_temporal(granule.get("umm", {}))
    if begin:
        return begin
    if end:
        return end

    single = (granule.get("umm", {}).get("TemporalExtent") or {}).get("SingleDateTime")
    return single if isinstance(single, str) and single else None


def _granule_extension(granule: Mapping[str, Any] | None) -> str | None:
    extension = _extension_from_url(_granule_data_url(granule))
    return extension


def _granule_data_url(granule: Mapping[str, Any] | None) -> str | None:
    if not granule:
        return None
    for related_url in granule.get("umm", {}).get("RelatedUrls", []) or []:
        url = related_url.get("URL") if isinstance(related_url, Mapping) else None
        if isinstance(url, str) and url:
            return url
    return None


def _assessed_asset_href(response: Mapping[str, Any] | None) -> str | None:
    if not response:
        return None
    return _href_from_example_assets(response.get("example_assets"))


def _href_from_example_assets(example_assets: Any) -> str | None:
    if isinstance(example_assets, str):
        return example_assets
    if isinstance(example_assets, Mapping):
        direct_href = _href_from_asset_mapping(example_assets)
        if direct_href:
            return direct_href
        for value in example_assets.values():
            href = _href_from_example_assets(value)
            if href:
                return href
    if isinstance(example_assets, Sequence) and not isinstance(example_assets, str):
        for value in example_assets:
            href = _href_from_example_assets(value)
            if href:
                return href
    return None


def _href_from_asset_mapping(asset: Mapping[str, Any]) -> str | None:
    for key in ("href", "direct_href", "url", "URL", "s3_url"):
        value = asset.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extension_from_url(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path
    if "." not in path:
        return None
    return path.rsplit(".", maxsplit=1)[-1].lower() or None


def _scheme_from_url(url: str | None) -> str | None:
    if not url:
        return None
    return urlparse(url).scheme or None


def _sampling_error_message(reason: str) -> str:
    messages = {
        "no_granule_found": "CMR did not return a sample granule for this collection.",
        "no_granule_ur": "Sampled granule did not include a GranuleUR.",
    }
    return messages.get(reason, reason)


def _diagnostic_fields(
    *,
    compatible: bool,
    failure_reason: Any,
    error_message: str | None,
    assessment: Mapping[str, Any],
    compatibility_response: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if compatible:
        return {
            "assessment_status": "compatible",
            "failure_stage": None,
            "failure_category": None,
            "failure_subcategory": None,
            "error_code": None,
            "error_detail": None,
            "failure_http_status_code": None,
            "failure_endpoint": None,
            "failure_url": None,
            "raw_error_body": None,
        }

    stage = _failure_stage(failure_reason)
    category, subcategory = _failure_classification(
        failure_reason, assessment, compatibility_response
    )
    raw_error_body = _raw_error_body(failure_reason, assessment, compatibility_response)
    failure_endpoint = _failure_endpoint(failure_reason, assessment)
    return {
        "assessment_status": _assessment_status(failure_reason, category, subcategory),
        "failure_stage": stage,
        "failure_category": category,
        "failure_subcategory": subcategory,
        "error_code": subcategory,
        "error_detail": error_message,
        "failure_http_status_code": _failure_http_status_code(
            failure_reason, assessment, compatibility_response
        ),
        "failure_endpoint": failure_endpoint,
        "failure_url": _failure_url(failure_reason, assessment),
        "raw_error_body": raw_error_body,
    }


def _failure_stage(failure_reason: Any) -> str | None:
    stages = {
        "no_granule_found": "sampling",
        "no_granule_ur": "sampling",
        "compatibility_request_failed": "compatibility",
        "xarray_no_tileable_variables": "compatibility",
        "missing_xy_spatial_coordinates": "compatibility",
        "granule_bbox_unavailable": "bbox_resolution",
        "bbox_request_failed": "bbox_probe",
        "assessment_exception": "assessment",
    }
    return stages.get(failure_reason, "assessment") if failure_reason else None


def _failure_classification(
    failure_reason: Any,
    assessment: Mapping[str, Any],
    compatibility_response: Mapping[str, Any] | None,
) -> tuple[str, str]:
    if failure_reason == "compatibility_request_failed":
        return _compatibility_failure_classification(compatibility_response)
    if failure_reason == "xarray_no_tileable_variables":
        return "data_incompatible", "no_tileable_variables"
    if failure_reason == "missing_xy_spatial_coordinates":
        return "data_incompatible", "missing_xy_spatial_coordinates"
    if failure_reason == "bbox_request_failed":
        return _bbox_failure_classification(assessment)
    if failure_reason == "granule_bbox_unavailable":
        return "metadata_incomplete", "invalid_or_missing_granule_bbox"
    if failure_reason in {"no_granule_found", "no_granule_ur"}:
        return "metadata_incomplete", str(failure_reason)
    if failure_reason == "assessment_exception":
        return "assessment_error", "assessment_exception"
    if failure_reason:
        return "assessment_error", str(failure_reason)
    return "assessment_error", "assessment_failed"


def _compatibility_failure_classification(
    compatibility_response: Mapping[str, Any] | None,
) -> tuple[str, str]:
    status_code = _status_code_from_compatibility(compatibility_response)
    body = _error_body_from_compatibility(compatibility_response) or ""
    body_lower = body.lower()

    if "media types not supported" in body_lower:
        return "unsupported_asset", "unsupported_media_type"
    if "direct_href" in body_lower and "field required" in body_lower:
        return "metadata_incomplete", "missing_asset_href_or_extension"
    if "169.254.169.254/latest/api/token" in body_lower:
        return "inaccessible", "s3_credential_lookup_failed"
    if "no variables are compatible" in body_lower:
        return "data_incompatible", "no_compatible_variables"
    if "unable to decode time units" in body_lower:
        return "data_incompatible", "decode_error"
    if "file signature not found" in body_lower:
        return "data_incompatible", "unsupported_file_signature"
    if "not recognized as being in a supported file format" in body_lower:
        return "data_incompatible", "unsupported_file_signature"
    if "could not open a sample granule" in body_lower and status_code == 400:
        return "data_incompatible", "cant_open_file"
    if status_code is None:
        return "data_incompatible", "no_compatible_backend_or_variables"
    if status_code >= 500:
        return "service_error", f"compatibility_http_{status_code}"
    if status_code >= 400:
        return "request_error", f"compatibility_http_{status_code}"
    return "data_incompatible", "invalid_compatibility_response"


def _bbox_failure_classification(assessment: Mapping[str, Any]) -> tuple[str, str]:
    status_code = assessment.get("bbox_status_code")
    body = str(assessment.get("bbox_error_body") or "").lower()
    if status_code == 204:
        return "no_rendered_content", "bbox_no_content"
    if "couldn't find x and y spatial coordinates" in body:
        return "data_incompatible", "missing_xy_spatial_coordinates"
    if "could not find x and y spatial coordinates" in body:
        return "data_incompatible", "missing_xy_spatial_coordinates"
    if "can only work with 2d or 3d dataset" in body:
        return "data_incompatible", "unsupported_dimensionality"
    if "y missing coordinates" in body:
        return "data_incompatible", "missing_y_coordinate"
    if "aoi is too large" in body:
        return "request_error", "aoi_too_large"
    if assessment.get("bbox_probe_limited"):
        return "render_error", "bbox_probe_attempt_limit_exceeded"
    if isinstance(status_code, int):
        if status_code >= 500:
            return "render_error", f"bbox_http_{status_code}"
        if status_code >= 400:
            return "request_error", f"bbox_http_{status_code}"
    return "render_error", "bbox_request_failed"


def _assessment_status(
    failure_reason: Any,
    category: str,
    subcategory: str,
) -> str:
    if failure_reason == "assessment_exception":
        return "assessment_error"
    if category in {
        "service_error",
        "metadata_incomplete",
        "render_error",
        "request_error",
        "no_rendered_content",
        "inaccessible",
    }:
        return "inconclusive"
    return "incompatible"


def _failure_http_status_code(
    failure_reason: Any,
    assessment: Mapping[str, Any],
    compatibility_response: Mapping[str, Any] | None,
) -> int | None:
    if failure_reason == "compatibility_request_failed":
        return _status_code_from_compatibility(compatibility_response)
    if failure_reason == "xarray_no_tileable_variables":
        return None
    if failure_reason == "missing_xy_spatial_coordinates":
        return None
    if failure_reason == "bbox_request_failed":
        status_code = assessment.get("bbox_status_code")
        return status_code if isinstance(status_code, int) else None
    return None


def _failure_endpoint(failure_reason: Any, assessment: Mapping[str, Any]) -> str | None:
    if failure_reason == "compatibility_request_failed":
        return "/compatibility"
    if failure_reason == "xarray_no_tileable_variables":
        return "/compatibility"
    if failure_reason == "missing_xy_spatial_coordinates":
        return "/compatibility"
    if failure_reason == "bbox_request_failed":
        backend = assessment.get("backend")
        return f"/{backend}/bbox" if isinstance(backend, str) else "/bbox"
    return None


def _failure_url(failure_reason: Any, assessment: Mapping[str, Any]) -> str | None:
    if failure_reason == "bbox_request_failed":
        return _string_or_none(assessment.get("bbox_url"))
    return None


def _raw_error_body(
    failure_reason: Any,
    assessment: Mapping[str, Any],
    compatibility_response: Mapping[str, Any] | None,
) -> str | None:
    if failure_reason == "compatibility_request_failed":
        return _error_body_from_compatibility(compatibility_response)
    if failure_reason == "xarray_no_tileable_variables":
        return _string_or_none(assessment.get("failure_detail"))
    if failure_reason == "missing_xy_spatial_coordinates":
        return _string_or_none(assessment.get("failure_detail"))
    if failure_reason == "bbox_request_failed":
        return _string_or_none(assessment.get("bbox_error_body"))
    return None


def _error_message(
    failure_reason: Any,
    assessment: Mapping[str, Any],
    compatibility_response: Mapping[str, Any] | None,
) -> str:
    failure_detail = _string_or_none(assessment.get("failure_detail"))
    if failure_detail:
        return failure_detail
    if failure_reason == "bbox_request_failed":
        status_code = assessment.get("bbox_status_code")
        if status_code == 204:
            return "BBox probe returned no rendered content (HTTP 204 No Content)."
        return f"BBox probe failed with HTTP status {status_code}."
    if failure_reason == "granule_bbox_unavailable":
        return "Could not derive a valid granule bounding box for the render probe."
    if failure_reason == "compatibility_request_failed":
        status_code = _status_code_from_compatibility(compatibility_response)
        body = _error_body_from_compatibility(compatibility_response)
        if status_code and body:
            return (
                f"Compatibility request failed with HTTP status {status_code}: "
                f"{_snippet(body)}"
            )
        if status_code:
            return f"Compatibility request failed with HTTP status {status_code}."
        return "Compatibility response did not include a usable backend and variables."
    if failure_reason:
        return str(failure_reason)
    return "Assessment failed without a specific failure reason."


def _status_code_from_compatibility(response: Mapping[str, Any] | None) -> int | None:
    if not response:
        return None
    status_code = response.get("status_code")
    return status_code if isinstance(status_code, int) else None


def _error_body_from_compatibility(response: Mapping[str, Any] | None) -> str | None:
    if not response:
        return None
    error = response.get("error")
    return str(error) if error is not None else None


def _tiles_url(
    assessment: Mapping[str, Any], response: Mapping[str, Any] | None
) -> str | None:
    bbox_url = _string_or_none(assessment.get("bbox_url"))
    if bbox_url:
        return bbox_url
    if not response:
        return None
    for link in _compatibility_links(response):
        href = _link_href(link)
        if href:
            return href
    return None


def _selected_variable(assessment: Mapping[str, Any]) -> str | None:
    variables = _list_or_none(assessment.get("variables"))
    if not variables:
        return None
    variable = variables[0]
    return str(variable) if variable is not None else None


def _compatibility_variables(response: Mapping[str, Any] | None) -> list[str] | None:
    if not response:
        return None
    variables = response.get("variables")
    if isinstance(variables, Mapping):
        return [str(variable) for variable in variables]
    if isinstance(variables, Sequence) and not isinstance(variables, str):
        return [str(variable) for variable in variables]
    return None


def _compatibility_links(response: Mapping[str, Any]) -> list[Any]:
    links = response.get("links") or response.get("tiles") or []
    if isinstance(links, Mapping):
        return list(links.values())
    if isinstance(links, list):
        return links
    return []


def _link_href(link: Any) -> str | None:
    if isinstance(link, str):
        return link
    if isinstance(link, Mapping):
        for key in ("href", "url", "rel"):
            value = link.get(key)
            if isinstance(value, str):
                return value
    return None


def _snippet(value: str, max_length: int = 500) -> str:
    return value if len(value) <= max_length else f"{value[:max_length]}..."


def _snippet_or_none(value: Any, max_length: int = 500) -> str | None:
    value = _string_or_none(value)
    return _snippet(value, max_length) if value is not None else None


def _string_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _list_or_none(value: Any) -> list[Any] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _sequence_or_none(value: Any) -> list[Any] | None:
    if value is None or isinstance(value, str):
        return None
    if isinstance(value, Sequence):
        return list(value)
    return None


def _serialize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    serialized = {column: row.get(column) for column in OUTPUT_COLUMNS}
    for column in NESTED_COLUMNS:
        if serialized[column] is not None:
            serialized[column] = json.dumps(
                serialized[column], sort_keys=True, default=str
            )
    return serialized


class _AssessmentProgressState:
    def __init__(self, total: int, callback: AssessmentProgressCallback | None) -> None:
        self.total = total
        self.callback = callback
        self.completed = 0
        self.compatible = 0
        self.incompatible = 0
        self.errors = 0
        self.in_flight: dict[str, None] = {}

    def emit(
        self,
        name: str,
        collection_concept_id: str | None = None,
        row: Mapping[str, Any] | None = None,
    ) -> None:
        tiling_compatible = _row_tiling_compatible(row)
        incompatible_reason = _row_incompatible_reason(row)
        _emit_progress(
            self.callback,
            AssessmentProgressEvent(
                name=name,
                collection_concept_id=collection_concept_id,
                total=self.total,
                completed=self.completed,
                compatible=self.compatible,
                incompatible=self.incompatible,
                errors=self.errors,
                in_flight=tuple(self.in_flight),
                tiling_compatible=tiling_compatible,
                incompatible_reason=incompatible_reason,
                error=incompatible_reason == "assessment_exception",
            ),
        )

    def start(self, collection_concept_id: str) -> None:
        self.in_flight[collection_concept_id] = None
        self.emit("collection_started", collection_concept_id)

    def finish(self, collection_concept_id: str, row: Mapping[str, Any]) -> None:
        self.in_flight.pop(collection_concept_id, None)
        self.completed += 1
        if _row_tiling_compatible(row):
            self.compatible += 1
        else:
            self.incompatible += 1
        if _row_incompatible_reason(row) == "assessment_exception":
            self.errors += 1
        self.emit("collection_finished", collection_concept_id, row)


def _row_tiling_compatible(row: Mapping[str, Any] | None) -> bool | None:
    if row is None:
        return None
    return bool(row.get("tiling_compatible"))


def _row_incompatible_reason(row: Mapping[str, Any] | None) -> str | None:
    if row is None:
        return None
    reason = row.get("incompatible_reason")
    return reason if isinstance(reason, str) else None


def _emit(progress: Callable[[str], None] | None, message: str) -> None:
    if progress:
        progress(message)


def _emit_progress(
    callback: AssessmentProgressCallback | None, event: AssessmentProgressEvent
) -> None:
    if callback:
        callback(event)
