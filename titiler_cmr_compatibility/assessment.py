"""API-first compatibility assessment helpers for TiTiler-CMR."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from httpx2 import AsyncClient

from .api import fetch_granule_by_ur
from .known_variables import known_variables
from .umm_helpers import parse_bounds_from_spatial, parse_temporal

DEV_TITILER_CMR_ENDPOINT = "https://v4jec6i5c0.execute-api.us-west-2.amazonaws.com"
DEFAULT_BBOX_SUBSET_FRACTION = 0.1
DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS = 10
DEFAULT_COMPATIBILITY_RETRY_DELAYS = (0.5, 1.5)
RETRYABLE_COMPATIBILITY_STATUS_CODES = {500, 502, 503, 504}
SPATIAL_DIMENSION_NAMES = {
    "x",
    "y",
    "lon",
    "lat",
    "longitude",
    "latitude",
    "xcoordinates",
    "ycoordinates",
}
TIME_DIMENSION_NAMES = {"time", "datetime", "date", "valid_time"}
BBox = tuple[float, float, float, float]
logger = logging.getLogger(__name__)


async def assess_collection_compatibility(
    collection_concept_id: str,
    granule_ur: str,
    titiler_cmr_endpoint: str = DEV_TITILER_CMR_ENDPOINT,
    client: AsyncClient | None = None,
    timeout: int = 60,
    granule_bbox: BBox | None = None,
    granule_temporal: str | None = None,
    bbox_subset_fraction: float = DEFAULT_BBOX_SUBSET_FRACTION,
    max_bbox_variable_attempts: int = DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS,
    compatibility_retry_delays: Sequence[float] = DEFAULT_COMPATIBILITY_RETRY_DELAYS,
) -> dict[str, Any]:
    """Assess whether one sampled granule can render through TiTiler-CMR.

    This calls `/compatibility`, retries transient failures and any returned
    `compatible_groups`, and then probes the granule's CMR bounding box through
    TiTiler-CMR's `/bbox` endpoint for the first valid compatibility response.
    If the full bbox is too large for rendering, smaller deterministic probes
    are attempted as a fallback.
    """

    endpoint = titiler_cmr_endpoint.rstrip("/")

    if client is None:
        async with AsyncClient(timeout=timeout) as client:
            return await _assess_with_client(
                endpoint,
                collection_concept_id,
                granule_ur,
                client,
                timeout,
                granule_bbox,
                bbox_subset_fraction,
                max_bbox_variable_attempts,
                compatibility_retry_delays,
                granule_temporal,
            )

    return await _assess_with_client(
        endpoint,
        collection_concept_id,
        granule_ur,
        client,
        timeout,
        granule_bbox,
        bbox_subset_fraction,
        max_bbox_variable_attempts,
        compatibility_retry_delays,
        granule_temporal,
    )


async def _assess_with_client(
    endpoint: str,
    collection_concept_id: str,
    granule_ur: str,
    client: AsyncClient,
    timeout: int,
    granule_bbox: BBox | None,
    bbox_subset_fraction: float,
    max_bbox_variable_attempts: int,
    compatibility_retry_delays: Sequence[float],
    granule_temporal: str | None,
) -> dict[str, Any]:
    compatibility = await _compatibility(
        endpoint,
        collection_concept_id,
        granule_ur,
        client,
        timeout,
        retry_delays=compatibility_retry_delays,
    )
    compatible_groups = compatibility.get("compatible_groups", []) or []
    selected_group = None
    tested_groups = []

    for group in compatible_groups:
        tested_groups.append(group)
        group_compatibility = await _compatibility(
            endpoint,
            collection_concept_id,
            granule_ur,
            client,
            timeout,
            group=group,
            retry_delays=compatibility_retry_delays,
        )
        if _is_valid_compatibility(group_compatibility):
            compatibility = group_compatibility
            selected_group = group
            break

    if not _is_valid_compatibility(compatibility):
        failure_reason, failure_detail = _invalid_compatibility_failure(
            compatibility,
            compatible_groups,
            tested_groups,
        )
        return {
            "collection_concept_id": collection_concept_id,
            "granule_ur": granule_ur,
            "compatible": False,
            "group": selected_group,
            "compatible_groups": compatible_groups,
            "tested_groups": tested_groups,
            "compatibility_response": compatibility,
            "failure_reason": failure_reason,
            "failure_detail": failure_detail,
        }

    backend = compatibility["backend"]
    needs_temporal = _needs_granule_temporal(compatibility, backend)
    full_bbox, resolved_granule_temporal = await _resolve_granule_context(
        collection_concept_id,
        granule_ur,
        granule_bbox,
        granule_temporal,
        needs_temporal,
        client,
        timeout,
    )
    probe_bboxes = _probe_bboxes(full_bbox, bbox_subset_fraction) if full_bbox else []
    if not probe_bboxes:
        return {
            "collection_concept_id": collection_concept_id,
            "granule_ur": granule_ur,
            "compatible": False,
            "backend": backend,
            "group": selected_group,
            "variables": [],
            "compatible_groups": compatible_groups,
            "tested_groups": tested_groups,
            "compatibility_response": compatibility,
            "granule_bbox": full_bbox,
            "probe_bbox": None,
            "bbox_status_code": None,
            "bbox_attempt_count": 0,
            "bbox_attempts": [],
            "bbox_probe_limited": False,
            "failure_reason": "granule_bbox_unavailable",
        }

    variable_attempts, variable_probe_limited = _bbox_variable_attempts(
        compatibility,
        backend,
        max_bbox_variable_attempts,
    )
    dimension_slice = _bbox_dimension_slice(
        compatibility, backend, resolved_granule_temporal
    )
    selected_attempt: dict[str, Any] | None = None
    last_attempt: dict[str, Any] | None = None
    bbox_attempts: list[dict[str, Any]] = []

    stop_bbox_attempts = False
    for variable_attempt in variable_attempts:
        should_try_smaller_bboxes = False
        for bbox_index, probe_bbox in enumerate(probe_bboxes):
            if bbox_index > 0 and not should_try_smaller_bboxes:
                break

            bbox_path = _format_bbox_path(probe_bbox)
            bbox_url = f"{endpoint}/{backend}/bbox/{bbox_path}.png"
            bbox_params = {
                "collection_concept_id": collection_concept_id,
                "granule_ur": granule_ur,
                **({"group": selected_group} if selected_group else {}),
                **(
                    {"variables": variable_attempt["variables"]}
                    if variable_attempt["variables"]
                    else {}
                ),
                **_bbox_dimension_slice_params(dimension_slice),
            }
            bbox_response = await client.get(
                bbox_url,
                params=bbox_params,
                timeout=timeout,
            )
            attempt = _bbox_attempt_record(
                _url_with_query(bbox_url, bbox_params),
                probe_bbox,
                selected_group,
                variable_attempt,
                dimension_slice,
                bbox_response.status_code,
                bbox_response.text,
            )
            bbox_attempts.append(attempt)
            last_attempt = attempt
            if _bbox_status_is_compatible(bbox_response.status_code):
                selected_attempt = attempt
                break
            if _bbox_error_is_terminal(bbox_response.status_code, bbox_response.text):
                stop_bbox_attempts = True
                break
            if _bbox_error_allows_smaller_probe(
                bbox_response.status_code,
                bbox_response.text,
            ):
                should_try_smaller_bboxes = True
        if selected_attempt or stop_bbox_attempts:
            break

    final_attempt = selected_attempt or last_attempt
    compatible = selected_attempt is not None
    bbox_error_body = None if compatible else _attempt_error_message(final_attempt)

    return {
        "collection_concept_id": collection_concept_id,
        "granule_ur": granule_ur,
        "compatible": compatible,
        "backend": backend,
        "group": selected_group,
        "variables": final_attempt["variables"] if final_attempt else [],
        "selected_variable_source": final_attempt["selected_variable_source"]
        if final_attempt
        else None,
        "dimension_selectors": final_attempt["dimension_selectors"]
        if final_attempt
        else [],
        "temporal": final_attempt["temporal"] if final_attempt else None,
        "selected_dimension_source": final_attempt["selected_dimension_source"]
        if final_attempt
        else None,
        "compatible_groups": compatible_groups,
        "tested_groups": tested_groups,
        "compatibility_response": compatibility,
        "granule_bbox": full_bbox,
        "probe_bbox": tuple(final_attempt["bbox"]) if final_attempt else probe_bboxes[0],
        "bbox_status_code": final_attempt["status_code"] if final_attempt else None,
        "bbox_error_body": bbox_error_body,
        "bbox_url": final_attempt["url"] if final_attempt else None,
        "bbox_attempt_count": len(bbox_attempts),
        "bbox_attempts": bbox_attempts,
        "bbox_probe_limited": variable_probe_limited,
        "failure_reason": None if compatible else "bbox_request_failed",
    }


async def _compatibility(
    endpoint: str,
    collection_concept_id: str,
    granule_ur: str,
    client: AsyncClient,
    timeout: int,
    group: str | None = None,
    retry_delays: Sequence[float] = DEFAULT_COMPATIBILITY_RETRY_DELAYS,
) -> dict[str, Any]:
    params = {
        "collection_concept_id": collection_concept_id,
        "granule_ur": granule_ur,
        "skip_variable_statistics": True,
        **({"group": group} if group else {}),
    }
    attempts = len(retry_delays) + 1
    response = None
    for attempt_index in range(attempts):
        response = await client.get(
            f"{endpoint}/compatibility", params=params, timeout=timeout
        )
        if response.status_code not in RETRYABLE_COMPATIBILITY_STATUS_CODES:
            break
        if attempt_index == attempts - 1:
            break
        await asyncio.sleep(max(0, retry_delays[attempt_index]))

    if response is None:
        msg = "Compatibility request did not run."
        return {"error": msg, "status_code": None}
    if response.status_code >= 400:
        return {"error": response.text, "status_code": response.status_code}
    return response.json()


async def _resolve_granule_context(
    collection_concept_id: str,
    granule_ur: str,
    granule_bbox: BBox | None,
    granule_temporal: str | None,
    needs_temporal: bool,
    client: AsyncClient,
    timeout: int,
) -> tuple[BBox | None, str | None]:
    """Return granule bbox and temporal metadata, fetching CMR only when needed."""
    normalized_bbox = _normalize_bbox(granule_bbox) if granule_bbox else None
    if granule_bbox is not None and (granule_temporal or not needs_temporal):
        return normalized_bbox, granule_temporal

    try:
        granule = await fetch_granule_by_ur(
            collection_concept_id, granule_ur, client=client, timeout=timeout
        )
    except Exception:
        if normalized_bbox:
            logger.warning(
                "Could not fetch granule temporal metadata for %s; falling back to "
                "collection temporal metadata if available.",
                granule_ur,
                exc_info=True,
            )
            return normalized_bbox, granule_temporal
        raise
    if not granule:
        return normalized_bbox, granule_temporal

    umm = granule.get("umm", {})
    resolved_bbox = normalized_bbox or _normalize_bbox(parse_bounds_from_spatial(umm))
    resolved_temporal = granule_temporal or _granule_temporal_value(umm)
    return resolved_bbox, resolved_temporal


def _granule_temporal_value(umm: Mapping[str, Any]) -> str | None:
    begin, end = parse_temporal(dict(umm))
    if begin:
        return begin
    if end:
        return end

    single = (umm.get("TemporalExtent") or {}).get("SingleDateTime")
    return single if isinstance(single, str) and single else None


def _normalize_bbox(bbox: Sequence[Any]) -> BBox | None:
    if len(bbox) != 4:
        return None

    try:
        west, south, east, north = (float(value) for value in bbox)
    except (TypeError, ValueError):
        return None

    if not all(math.isfinite(value) for value in (west, south, east, north)):
        return None
    if east <= west or north <= south:
        return None
    return west, south, east, north


def _probe_bboxes(bbox: BBox, fraction: float) -> list[BBox]:
    """Return deterministic render probe bboxes for a granule extent."""
    center_bbox = _subset_bbox(bbox, fraction)
    if not center_bbox:
        return []
    if not _needs_multi_probe_coverage(bbox):
        return list(dict.fromkeys([bbox, center_bbox]))

    west, south, east, north = bbox
    width = east - west
    height = north - south
    half_width = width * min(fraction, 0.05) / 2
    half_height = height * min(fraction, 0.05) / 2
    centers = [
        ((west + east) / 2, (south + north) / 2),
        (west + width * 0.25, south + height * 0.25),
        (west + width * 0.25, south + height * 0.75),
        (west + width * 0.75, south + height * 0.25),
        (west + width * 0.75, south + height * 0.75),
    ]
    bboxes = [bbox, center_bbox] + [
        (
            center_x - half_width,
            center_y - half_height,
            center_x + half_width,
            center_y + half_height,
        )
        for center_x, center_y in centers
    ]
    return list(dict.fromkeys(bboxes))


def _needs_multi_probe_coverage(bbox: BBox) -> bool:
    west, south, east, north = bbox
    width = east - west
    height = north - south
    aspect_ratio = max(width / height, height / width)
    return width >= 300 or height >= 120 or aspect_ratio >= 8


def _subset_bbox(bbox: BBox, fraction: float) -> BBox | None:
    if not 0 < fraction <= 1:
        return None

    west, south, east, north = bbox
    center_x = (west + east) / 2
    center_y = (south + north) / 2
    half_width = (east - west) * fraction / 2
    half_height = (north - south) * fraction / 2
    return (
        center_x - half_width,
        center_y - half_height,
        center_x + half_width,
        center_y + half_height,
    )


def _format_bbox_path(bbox: BBox) -> str:
    return ",".join(f"{coordinate:.12g}" for coordinate in bbox)


def _is_valid_compatibility(response: dict[str, Any]) -> bool:
    backend = response.get("backend")
    if backend == "rasterio":
        return True
    if backend != "xarray":
        return False
    if _has_empty_tileable_variables(response):
        return False
    return bool(_variables(response))


def _has_empty_tileable_variables(response: Mapping[str, Any]) -> bool:
    tileable_variables = response.get("tileable_variables")
    return isinstance(tileable_variables, list) and len(tileable_variables) == 0


def _invalid_compatibility_failure(
    response: Mapping[str, Any],
    compatible_groups: Sequence[str],
    tested_groups: Sequence[str],
) -> tuple[str, str]:
    """Return a clear failure reason for an unusable compatibility response."""
    if response.get("backend") == "xarray":
        if _has_empty_tileable_variables(response):
            return (
                "missing_xy_spatial_coordinates",
                "The compatibility endpoint opened the sample asset with the xarray "
                "backend, but tileable_variables was empty, indicating no variables "
                "have recognized x/y spatial coordinates.",
            )
        if not _variables(response):
            if compatible_groups:
                detail = (
                    "The compatibility endpoint opened the sample asset with the xarray "
                    "backend, but neither the root response nor the tested groups exposed "
                    "any tileable variables."
                )
                if tested_groups:
                    detail = f"{detail} Tested groups: {', '.join(tested_groups)}."
                return "xarray_no_tileable_variables", detail
            return (
                "xarray_no_tileable_variables",
                "The compatibility endpoint opened the sample asset with the xarray "
                "backend, but it returned no tileable variables and no compatible groups "
                "to inspect.",
            )

    if response.get("status_code") is not None:
        return (
            "compatibility_request_failed",
            f"The compatibility endpoint returned HTTP {response['status_code']}.",
        )

    return (
        "compatibility_response_invalid",
        "The compatibility endpoint response did not identify a usable rasterio "
        "backend or xarray variables for bbox probing.",
    )


def _variables(response: Mapping[str, Any]) -> list[str]:
    variables = response.get("variables") or {}
    if isinstance(variables, dict):
        return list(variables)
    if isinstance(variables, list):
        return [str(variable) for variable in variables]
    return []


def _bbox_variable_attempts(
    compatibility: dict[str, Any],
    backend: str,
    max_attempts: int,
) -> tuple[list[dict[str, Any]], bool]:
    if backend != "xarray":
        return [{"variables": [], "selected_variable_source": "rasterio_no_variable"}], False

    available_variables = _bbox_candidate_variables(compatibility)
    compatibility_link_variables = _compatibility_link_variables(compatibility)
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    for variable in compatibility_link_variables:
        if variable in available_variables:
            _append_variable_candidate(candidates, seen, variable, "compatibility_link")

    ranked_available = [variable for variable in known_variables if variable in available_variables]
    for variable in ranked_available:
        _append_variable_candidate(candidates, seen, variable, "fallback_ranked")

    for variable in available_variables:
        _append_variable_candidate(candidates, seen, variable, "fallback_exhaustive")

    bounded_max_attempts = max(1, max_attempts)
    return candidates[:bounded_max_attempts], len(candidates) > bounded_max_attempts


def _bbox_candidate_variables(response: Mapping[str, Any]) -> list[str]:
    tileable_variables = response.get("tileable_variables")
    if isinstance(tileable_variables, list) and tileable_variables:
        return [str(variable) for variable in tileable_variables]
    return _variables(response)


def _append_variable_candidate(
    candidates: list[dict[str, Any]], seen: set[str], variable: str, source: str
) -> None:
    if variable in seen:
        return
    seen.add(variable)
    candidates.append({"variables": [variable], "selected_variable_source": source})


def _compatibility_link_variables(response: dict[str, Any]) -> list[str]:
    variables: list[str] = []
    for link in _compatibility_links(response):
        href = _link_href(link)
        if not href:
            continue
        parsed_query = parse_qs(urlparse(href).query)
        for raw_value in parsed_query.get("variables", []):
            variables.extend(_split_variables_query_value(raw_value))
    return list(dict.fromkeys(variables))


def _compatibility_links(response: dict[str, Any]) -> list[Any]:
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


def _split_variables_query_value(value: str) -> list[str]:
    return [variable for variable in value.split(",") if variable]


def _needs_granule_temporal(compatibility: Mapping[str, Any], backend: str) -> bool:
    """Return whether bbox probing needs the sampled granule's temporal value."""
    if backend != "xarray":
        return False
    dimensions = compatibility.get("dimensions") or {}
    if isinstance(dimensions, Mapping) and any(
        str(dimension).lower() in TIME_DIMENSION_NAMES for dimension in dimensions
    ):
        return True
    return _compatibility_requires_temporal(compatibility)


def _compatibility_requires_temporal(compatibility: Mapping[str, Any]) -> bool:
    for link in _compatibility_links(dict(compatibility)):
        href = _link_href(link)
        if href and "temporal={temporal}" in href:
            return True
    return False


def _bbox_dimension_slice(
    compatibility: Mapping[str, Any], backend: str, granule_temporal: str | None
) -> dict[str, Any]:
    """Return dimension selectors needed to reduce xarray variables to tileable arrays."""
    if backend != "xarray":
        return {"sel": [], "temporal": None, "selected_dimension_source": None}

    dimensions = compatibility.get("dimensions") or {}
    if not isinstance(dimensions, Mapping):
        dimensions = {}

    selectors: list[str] = []
    sources: list[str] = []
    temporal: str | None = None
    for dimension, size in dimensions.items():
        if _is_spatial_dimension(str(dimension)):
            continue
        if _is_singleton_dimension(size):
            continue
        value, source = _dimension_selection_value(
            str(dimension), compatibility, granule_temporal
        )
        if value is None or source is None:
            continue
        if source in {"granule_temporal_datetime", "collection_temporal_datetime"}:
            selectors.append(f"{dimension}=nearest::{{datetime}}")
            temporal = value
        else:
            selectors.append(f"{dimension}=nearest::{value}")
        sources.append(f"{dimension}:{source}")

    if temporal is None and _compatibility_requires_temporal(compatibility):
        value, source = _temporal_selection_value(compatibility, granule_temporal)
        if value is not None and source is not None:
            temporal = value
            sources.append(f"temporal:{source}")

    return {
        "sel": selectors,
        "temporal": temporal,
        "selected_dimension_source": ",".join(sources) if sources else None,
    }


def _is_spatial_dimension(dimension: str) -> bool:
    return dimension.lower() in SPATIAL_DIMENSION_NAMES


def _is_singleton_dimension(size: Any) -> bool:
    try:
        return int(size) <= 1
    except (TypeError, ValueError):
        return False


def _dimension_selection_value(
    dimension: str, compatibility: Mapping[str, Any], granule_temporal: str | None
) -> tuple[str | None, str | None]:
    if dimension.lower() in TIME_DIMENSION_NAMES:
        return _temporal_selection_value(compatibility, granule_temporal)

    coordinates = compatibility.get("coordinates") or {}
    coordinate = coordinates.get(dimension) if isinstance(coordinates, Mapping) else None
    if not isinstance(coordinate, Mapping):
        return None, None

    minimum = _finite_float(coordinate.get("min"))
    maximum = _finite_float(coordinate.get("max"))
    if minimum is not None and maximum is not None:
        return _format_selector_value((minimum + maximum) / 2), "coordinate_midpoint"
    if minimum is not None:
        return _format_selector_value(minimum), "coordinate_min"
    if maximum is not None:
        return _format_selector_value(maximum), "coordinate_max"
    return None, None


def _temporal_selection_value(
    compatibility: Mapping[str, Any], granule_temporal: str | None
) -> tuple[str | None, str | None]:
    if granule_temporal:
        return granule_temporal, "granule_temporal_datetime"
    temporal = _compatibility_temporal_value(compatibility)
    if temporal:
        return temporal, "collection_temporal_datetime"
    return None, None


def _compatibility_temporal_value(compatibility: Mapping[str, Any]) -> str | None:
    datetime_entries = compatibility.get("datetime") or []
    if not isinstance(datetime_entries, Sequence) or isinstance(datetime_entries, str):
        return None

    for entry in datetime_entries:
        if not isinstance(entry, Mapping):
            continue
        for range_datetime in entry.get("RangeDateTimes") or []:
            if not isinstance(range_datetime, Mapping):
                continue
            beginning = range_datetime.get("BeginningDateTime")
            if isinstance(beginning, str) and beginning:
                return beginning
        single = entry.get("SingleDateTime")
        if isinstance(single, str) and single:
            return single
    return None


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _format_selector_value(value: float) -> str:
    return f"{value:.12g}"


def _bbox_dimension_slice_params(dimension_slice: Mapping[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if dimension_slice.get("sel"):
        params["sel"] = list(dimension_slice["sel"])
    if dimension_slice.get("temporal"):
        params["temporal"] = dimension_slice["temporal"]
    return params


def _url_with_query(url: str, params: Mapping[str, Any]) -> str:
    query = urlencode(params, doseq=True)
    return f"{url}?{query}" if query else url


def _bbox_attempt_record(
    url: str,
    bbox: BBox,
    group: str | None,
    variable_attempt: Mapping[str, Any],
    dimension_slice: Mapping[str, Any],
    status_code: int,
    response_text: str,
) -> dict[str, Any]:
    error_message = _bbox_error_message(status_code, response_text)
    return {
        "variables": list(variable_attempt["variables"]),
        "selected_variable_source": variable_attempt["selected_variable_source"],
        "dimension_selectors": list(dimension_slice["sel"]),
        "temporal": dimension_slice["temporal"],
        "selected_dimension_source": dimension_slice["selected_dimension_source"],
        "group": group,
        "bbox": list(bbox),
        "status_code": status_code,
        "error_snippet": _snippet(error_message) if error_message else None,
        "url": url,
    }


def _bbox_status_is_compatible(status_code: int) -> bool:
    return status_code == 200


def _bbox_error_message(status_code: int, response_text: str) -> str | None:
    if status_code == 204:
        return "BBox probe returned no rendered content (HTTP 204 No Content)."
    if status_code >= 400:
        return response_text
    if status_code != 200:
        return f"BBox probe returned unexpected HTTP status {status_code}."
    return None


def _bbox_error_is_terminal(status_code: int, response_text: str) -> bool:
    if status_code < 400:
        return False
    body = response_text.lower()
    return (
        "couldn't find x and y spatial coordinates" in body
        or "x missing coordinates" in body
        or "y missing coordinates" in body
    )


def _bbox_error_allows_smaller_probe(status_code: int, response_text: str) -> bool:
    if status_code < 400:
        return False
    body = response_text.lower()
    return (
        status_code == 503
        or "aoi is too large" in body
        or "array is too large" in body
        or "maximum array limit" in body
        or "too many pixels" in body
    )


def _attempt_error_message(attempt: Mapping[str, Any] | None) -> str | None:
    if not attempt:
        return None
    error_snippet = attempt.get("error_snippet")
    return str(error_snippet) if error_snippet else None


def _snippet(value: str, max_length: int = 500) -> str:
    return value if len(value) <= max_length else f"{value[:max_length]}..."
