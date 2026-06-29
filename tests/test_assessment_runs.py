import asyncio

import pandas as pd
import pytest

from titiler_cmr_compatibility import assessment_runs


COLLECTION = {
    "meta": {"concept-id": "C123-TEST"},
    "umm": {
        "ShortName": "SAMPLE",
        "Version": "001",
        "DataCenters": [{"ShortName": "LPDAAC"}],
        "ArchiveAndDistributionInformation": {
            "FileArchiveInformation": {"Format": "NETCDF-4"}
        },
        "ProcessingLevel": {"Id": "L2"},
    },
}

GRANULE = {
    "meta": {"concept-id": "GID123"},
    "umm": {
        "GranuleUR": "G123",
        "RelatedUrls": [{"URL": "s3://example-bucket/granule.nc"}],
        "SpatialExtent": {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "BoundingRectangles": [
                        {
                            "WestBoundingCoordinate": -10,
                            "SouthBoundingCoordinate": -20,
                            "EastBoundingCoordinate": 10,
                            "NorthBoundingCoordinate": 20,
                        }
                    ]
                }
            }
        },
        "TemporalExtent": {
            "RangeDateTime": {"BeginningDateTime": "2020-01-02T03:04:05.000Z"}
        },
    },
}


def test_normalize_assessment_row_preserves_legacy_and_audit_columns():
    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=42,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "backend": "xarray",
            "group": "/science",
            "variables": ["temperature"],
            "dimension_selectors": ["time=nearest::{datetime}"],
            "temporal": "1980-01-01T00:00:00.000Z",
            "selected_dimension_source": "time:granule_temporal_datetime",
            "compatible_groups": ["/science", "/backup"],
            "tested_groups": ["/science"],
            "compatibility_response": {
                "backend": "xarray",
                "variables": {"temperature": {}},
                "example_assets": [
                    {"href": "s3://assessed-bucket/path/granule.h5"}
                ],
            },
            "granule_bbox": (-10.0, -20.0, 10.0, 20.0),
            "probe_bbox": (-1.0, -2.0, 1.0, 2.0),
            "bbox_status_code": 500,
            "bbox_error_body": "render failed",
            "bbox_url": "https://titiler.example.test/xarray/bbox/-1,-2,1,2.png",
            "bbox_attempt_count": 1,
            "bbox_attempts": [
                {
                    "variables": ["temperature"],
                    "group": "/science",
                    "status_code": 500,
                    "error_snippet": "render failed",
                }
            ],
            "failure_reason": "bbox_request_failed",
        },
    )

    assert row["collection_concept_id"] == "C123-TEST"
    assert row["collection_short_name_and_version"] == "SAMPLE.001"
    assert row["concept_id"] == "GID123"
    assert row["data_center"] == "LPDAAC"
    assert row["data_url"] == "s3://assessed-bucket/path/granule.h5"
    assert row["backend"] == "xarray"
    assert row["format"] == "NETCDF-4"
    assert row["extension"] == "nc"
    assert row["assessed_asset_href"] == "s3://assessed-bucket/path/granule.h5"
    assert row["assessed_asset_extension"] == "h5"
    assert row["assessed_asset_scheme"] == "s3"
    assert row["tiling_compatible"] is False
    assert row["incompatible_reason"] == "bbox_request_failed"
    assert row["error_message"] == "BBox probe failed with HTTP status 500."
    assert row["assessment_status"] == "inconclusive"
    assert row["failure_stage"] == "bbox_probe"
    assert row["failure_category"] == "render_error"
    assert row["failure_subcategory"] == "bbox_http_500"
    assert row["error_code"] == "bbox_http_500"
    assert row["failure_http_status_code"] == 500
    assert row["failure_endpoint"] == "/xarray/bbox"
    assert row["failure_url"] == "https://titiler.example.test/xarray/bbox/-1,-2,1,2.png"
    assert row["raw_error_body"] == "render failed"
    assert row["processing_level"] == "L2"
    assert row["tiles_url"] == "https://titiler.example.test/xarray/bbox/-1,-2,1,2.png"
    assert row["variable"] == "temperature"
    assert row["data_variables"] == ["temperature"]
    assert row["num_granules"] == 42
    assert row["groups"] == ["/science", "/backup"]
    assert row["granule_concept_id"] == "GID123"
    assert row["granule_ur"] == "G123"
    assert row["backend"] == "xarray"
    assert row["group"] == "/science"
    assert row["variables"] == ["temperature"]
    assert row["dimension_selectors"] == ["time=nearest::{datetime}"]
    assert row["temporal"] == "1980-01-01T00:00:00.000Z"
    assert row["selected_dimension_source"] == "time:granule_temporal_datetime"
    assert row["compatible_groups"] == ["/science", "/backup"]
    assert row["tested_groups"] == ["/science"]
    assert row["bbox_status_code"] == 500
    assert row["bbox_error_body"] == "render failed"
    assert row["bbox_error_snippet"] == "render failed"
    assert row["bbox_attempt_count"] == 1
    assert row["bbox_attempts"][0]["variables"] == ["temperature"]


def test_normalize_missing_granule_ur_row_has_expected_failure_columns():
    row = assessment_runs.normalize_missing_granule_row(
        COLLECTION, {"umm": {}}, 0, "no_granule_ur"
    )

    assert row["collection_concept_id"] == "C123-TEST"
    assert row["tiling_compatible"] is False
    assert row["incompatible_reason"] == "no_granule_ur"
    assert row["error_message"] == "Sampled granule did not include a GranuleUR."
    assert row["assessment_status"] == "inconclusive"
    assert row["failure_stage"] == "sampling"
    assert row["failure_category"] == "metadata_incomplete"
    assert row["failure_subcategory"] == "no_granule_ur"
    assert row["backend"] is None
    assert "collection_short_name_and_version" in row
    assert "format" in row


def test_normalize_assessment_row_classifies_empty_tileable_variables_as_missing_xy():
    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=42,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "backend": "xarray",
            "compatibility_response": {
                "backend": "xarray",
                "tileable_variables": [],
                "variables": {"temperature": {}},
            },
            "failure_reason": "missing_xy_spatial_coordinates",
            "failure_detail": "No variables have recognized x/y spatial coordinates.",
        },
    )

    assert row["incompatible_reason"] == "missing_xy_spatial_coordinates"
    assert row["failure_stage"] == "compatibility"
    assert row["failure_category"] == "data_incompatible"
    assert row["failure_subcategory"] == "missing_xy_spatial_coordinates"
    assert row["failure_endpoint"] == "/compatibility"
    assert row["raw_error_body"] == "No variables have recognized x/y spatial coordinates."


def test_normalize_assessment_row_classifies_compatibility_error_body():
    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=1,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "compatibility_response": {
                "error": "{\"detail\": \"No valid asset found. Asset's media types not supported\"}",
                "status_code": 400,
            },
            "failure_reason": "compatibility_request_failed",
        },
    )

    assert row["assessment_status"] == "incompatible"
    assert row["failure_stage"] == "compatibility"
    assert row["failure_category"] == "unsupported_asset"
    assert row["failure_subcategory"] == "unsupported_media_type"
    assert row["failure_http_status_code"] == 400
    assert row["failure_endpoint"] == "/compatibility"
    assert row["raw_error_body"] == row["compatibility_error_body"]


def test_normalize_assessment_row_classifies_aws_metadata_credential_error():
    body = "Failed to PUT 169.254.169.254/latest/api/token while opening s3 asset"

    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=1,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "compatibility_response": {"error": body, "status_code": 500},
            "failure_reason": "compatibility_request_failed",
        },
    )

    assert row["assessment_status"] == "inconclusive"
    assert row["failure_stage"] == "compatibility"
    assert row["failure_category"] == "inaccessible"
    assert row["failure_subcategory"] == "s3_credential_lookup_failed"
    assert row["raw_error_body"] == body


@pytest.mark.parametrize(
    ("body", "subcategory"),
    [
        ("Unable to synchronously open file (file signature not found)", "unsupported_file_signature"),
        ("unable to decode time units 'TET is the number of atomic seconds'", "decode_error"),
        ("Could not open a sample granule with either backend", "cant_open_file"),
    ],
)
def test_normalize_assessment_row_classifies_compatibility_400_data_errors(body, subcategory):
    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=1,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "compatibility_response": {"error": body, "status_code": 400},
            "failure_reason": "compatibility_request_failed",
        },
    )

    assert row["assessment_status"] == "incompatible"
    assert row["failure_category"] == "data_incompatible"
    assert row["failure_subcategory"] == subcategory


def test_normalize_assessment_row_classifies_xarray_without_tileable_variables():
    detail = (
        "The compatibility endpoint opened the sample asset with the xarray backend, "
        "but it returned no tileable variables and no compatible groups to inspect."
    )

    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=1,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "compatibility_response": {
                "backend": "xarray",
                "variables": {},
                "compatible_groups": [],
            },
            "failure_reason": "xarray_no_tileable_variables",
            "failure_detail": detail,
        },
    )

    assert row["assessment_status"] == "incompatible"
    assert row["failure_stage"] == "compatibility"
    assert row["failure_category"] == "data_incompatible"
    assert row["failure_subcategory"] == "no_tileable_variables"
    assert row["failure_endpoint"] == "/compatibility"
    assert row["error_message"] == detail
    assert row["raw_error_body"] == detail


def test_normalize_assessment_row_classifies_204_bbox_as_inconclusive():
    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=1,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "backend": "rasterio",
            "bbox_status_code": 204,
            "bbox_error_body": "BBox probe returned no rendered content (HTTP 204 No Content).",
            "failure_reason": "bbox_request_failed",
        },
    )

    assert row["tiling_compatible"] is False
    assert row["assessment_status"] == "inconclusive"
    assert row["failure_stage"] == "bbox_probe"
    assert row["failure_category"] == "no_rendered_content"
    assert row["failure_subcategory"] == "bbox_no_content"
    assert row["error_message"] == "BBox probe returned no rendered content (HTTP 204 No Content)."


@pytest.mark.parametrize(
    ("body", "subcategory"),
    [
        (
            "Couldn't find X and Y spatial coordinates in dataset",
            "missing_xy_spatial_coordinates",
        ),
        (
            "titiler.xarray can only work with 2D or 3D dataset",
            "unsupported_dimensionality",
        ),
        ("y missing coordinates", "missing_y_coordinate"),
    ],
)
def test_normalize_assessment_row_classifies_known_bbox_500_patterns(body, subcategory):
    row = assessment_runs.normalize_assessment_row(
        collection=COLLECTION,
        granule=GRANULE,
        num_granules=1,
        assessment={
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "compatible": False,
            "backend": "xarray",
            "bbox_status_code": 500,
            "bbox_error_body": body,
            "failure_reason": "bbox_request_failed",
        },
    )

    assert row["assessment_status"] == "incompatible"
    assert row["failure_category"] == "data_incompatible"
    assert row["failure_subcategory"] == subcategory


def test_assess_collections_returns_one_row_per_collection(monkeypatch):
    calls = []

    async def fake_fetch_random_granule_metadata(
        collection_concept_id, client=None, timeout=30
    ):
        calls.append(("sample", collection_concept_id, timeout))
        return GRANULE, 42

    async def fake_assess_collection_compatibility(
        collection_concept_id,
        granule_ur,
        titiler_cmr_endpoint,
        client=None,
        timeout=60,
        granule_bbox=None,
        granule_temporal=None,
        max_bbox_variable_attempts=10,
    ):
        calls.append(
            (
                "assess",
                collection_concept_id,
                granule_ur,
                titiler_cmr_endpoint,
                timeout,
                granule_bbox,
                granule_temporal,
            )
        )
        return {
            "collection_concept_id": collection_concept_id,
            "granule_ur": granule_ur,
            "compatible": True,
            "backend": "rasterio",
            "variables": [],
            "compatible_groups": [],
            "tested_groups": [],
            "compatibility_response": {"backend": "rasterio"},
            "granule_bbox": granule_bbox,
            "probe_bbox": (-1.0, -2.0, 1.0, 2.0),
            "bbox_status_code": 200,
            "failure_reason": None,
        }

    monkeypatch.setattr(
        assessment_runs,
        "fetch_random_granule_metadata",
        fake_fetch_random_granule_metadata,
    )
    monkeypatch.setattr(
        assessment_runs,
        "assess_collection_compatibility",
        fake_assess_collection_compatibility,
    )

    rows = asyncio.run(
        assessment_runs.assess_collections(
            [COLLECTION, COLLECTION],
            titiler_cmr_endpoint="https://titiler.example.test",
            timeout=5,
            concurrency=2,
        )
    )

    assert len(rows) == 1
    assert rows[0]["tiling_compatible"] is True
    assert rows[0]["assessment_status"] == "compatible"
    assert rows[0]["collection_concept_id"] == "C123-TEST"
    assert calls == [
        ("sample", "C123-TEST", 5),
        (
            "assess",
            "C123-TEST",
            "G123",
            "https://titiler.example.test",
            5,
            (-10.0, -20.0, 10.0, 20.0),
            "2020-01-02T03:04:05.000Z",
        ),
    ]


def test_assess_collections_keeps_collection_local_exceptions_as_rows(monkeypatch):
    collection_two = {**COLLECTION, "meta": {"concept-id": "C456-TEST"}}

    async def fake_fetch_random_granule_metadata(
        collection_concept_id, client=None, timeout=30
    ):
        return GRANULE, 1

    async def fake_assess_collection_compatibility(**kwargs):
        if kwargs["collection_concept_id"] == "C123-TEST":
            raise RuntimeError("boom")
        return {
            "collection_concept_id": kwargs["collection_concept_id"],
            "granule_ur": kwargs["granule_ur"],
            "compatible": True,
        }

    monkeypatch.setattr(
        assessment_runs,
        "fetch_random_granule_metadata",
        fake_fetch_random_granule_metadata,
    )
    monkeypatch.setattr(
        assessment_runs,
        "assess_collection_compatibility",
        fake_assess_collection_compatibility,
    )

    rows = asyncio.run(
        assessment_runs.assess_collections([COLLECTION, collection_two], concurrency=2)
    )

    assert [row["collection_concept_id"] for row in rows] == ["C123-TEST", "C456-TEST"]
    assert rows[0]["tiling_compatible"] is False
    assert rows[0]["incompatible_reason"] == "assessment_exception"
    assert rows[0]["error_message"] == "boom"
    assert rows[1]["tiling_compatible"] is True


def test_assess_collections_emits_structured_progress_events(monkeypatch):
    collection_two = {**COLLECTION, "meta": {"concept-id": "C456-TEST"}}
    events = []

    async def fake_fetch_random_granule_metadata(
        collection_concept_id, client=None, timeout=30
    ):
        return GRANULE, 1

    async def fake_assess_collection_compatibility(**kwargs):
        return {
            "collection_concept_id": kwargs["collection_concept_id"],
            "granule_ur": kwargs["granule_ur"],
            "compatible": kwargs["collection_concept_id"] == "C123-TEST",
            "failure_reason": None
            if kwargs["collection_concept_id"] == "C123-TEST"
            else "bbox_request_failed",
        }

    monkeypatch.setattr(
        assessment_runs,
        "fetch_random_granule_metadata",
        fake_fetch_random_granule_metadata,
    )
    monkeypatch.setattr(
        assessment_runs,
        "assess_collection_compatibility",
        fake_assess_collection_compatibility,
    )

    asyncio.run(
        assessment_runs.assess_collections(
            [COLLECTION, collection_two],
            concurrency=1,
            progress_callback=events.append,
        )
    )

    assert [event.name for event in events] == [
        "batch_started",
        "collection_started",
        "collection_finished",
        "collection_started",
        "collection_finished",
        "batch_completed",
    ]
    assert events[1].collection_concept_id == "C123-TEST"
    assert events[1].in_flight == ("C123-TEST",)
    assert events[2].completed == 1
    assert events[2].compatible == 1
    assert events[2].incompatible == 0
    assert events[4].completed == 2
    assert events[4].compatible == 1
    assert events[4].incompatible == 1
    assert events[4].incompatible_reason == "bbox_request_failed"
    assert events[5].completed == 2


def test_assess_collections_rejects_invalid_concurrency():
    with pytest.raises(ValueError, match="concurrency"):
        asyncio.run(assessment_runs.assess_collections([COLLECTION], concurrency=0))


def test_write_assessment_parquet_round_trips_legacy_columns_and_nested_details(
    tmp_path,
):
    output = tmp_path / "nested" / "results.parquet"
    rows = [
        assessment_runs.normalize_assessment_row(
            collection=COLLECTION,
            granule=GRANULE,
            num_granules=1,
            assessment={
                "collection_concept_id": "C123-TEST",
                "granule_ur": "G123",
                "compatible": False,
                "compatible_groups": ["/science"],
                "compatibility_response": {"error": "bad", "status_code": 500},
                "bbox_attempt_count": 1,
                "bbox_attempts": [
                    {
                        "variables": ["temperature"],
                        "group": None,
                        "status_code": 500,
                        "error_snippet": "bad",
                    }
                ],
                "failure_reason": "compatibility_request_failed",
            },
        )
    ]

    assessment_runs.write_assessment_parquet(rows, output)

    frame = pd.read_parquet(output)
    assert (
        list(frame.columns[: len(assessment_runs.LEGACY_COLUMNS)])
        == assessment_runs.LEGACY_COLUMNS
    )
    assert set(assessment_runs.LEGACY_COLUMNS).issubset(frame.columns)
    assert frame.loc[0, "collection_concept_id"] == "C123-TEST"
    assert frame.loc[0, "collection_short_name_and_version"] == "SAMPLE.001"
    assert frame.loc[0, "concept_id"] == "GID123"
    assert frame.loc[0, "data_url"] == "s3://example-bucket/granule.nc"
    assert bool(frame.loc[0, "tiling_compatible"]) is False
    assert frame.loc[0, "incompatible_reason"] == "compatibility_request_failed"
    assert frame.loc[0, "assessment_status"] == "inconclusive"
    assert frame.loc[0, "failure_category"] == "service_error"
    assert frame.loc[0, "failure_subcategory"] == "compatibility_http_500"
    assert frame.loc[0, "bbox_attempt_count"] == 1
    assert "temperature" in frame.loc[0, "bbox_attempts"]
    assert "bad" in frame.loc[0, "compatibility_response"]


def test_write_empty_assessment_parquet_has_expected_columns(tmp_path):
    output = tmp_path / "empty.parquet"

    assessment_runs.write_assessment_parquet([], output)

    frame = pd.read_parquet(output)
    assert frame.empty
    assert list(frame.columns) == assessment_runs.OUTPUT_COLUMNS



def test_run_batch_assessment_rejects_limit_with_explicit_collection_ids(tmp_path):
    with pytest.raises(ValueError, match="limit cannot be combined"):
        asyncio.run(
            assessment_runs.run_batch_assessment(
                output_path=tmp_path / "results.parquet",
                limit=1,
                collection_concept_ids=["C123-TEST"],
            )
        )
