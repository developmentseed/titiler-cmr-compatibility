import asyncio

from titiler_cmr_compatibility.assessment import assess_collection_compatibility


class FakeResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.text)


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def get(self, url, params, timeout, **kwargs):
        self.calls.append({"url": url, "params": params, "timeout": timeout, **kwargs})
        return self.responses.pop(0)


def test_assess_collection_compatibility_uses_bbox_for_xarray_with_first_working_variable():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "variables": {
                        "temperature": {"shape": [10, 10], "dtype": "float32"},
                        "quality": {"shape": [10, 10], "dtype": "uint8"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is True
    assert result["backend"] == "xarray"
    assert result["group"] is None
    assert result["variables"] == ["temperature"]
    assert result["granule_bbox"] == (-10.0, -20.0, 10.0, 20.0)
    assert result["probe_bbox"] == (-10.0, -20.0, 10.0, 20.0)
    assert result["bbox_status_code"] == 200
    assert result["failure_reason"] is None
    assert client.calls == [
        {
            "url": "https://titiler.example.test/compatibility",
            "params": {
                "collection_concept_id": "C123-TEST",
                "granule_ur": "G123",
                "skip_variable_statistics": True,
            },
            "timeout": 5,
        },
        {
            "url": "https://titiler.example.test/xarray/bbox/-10,-20,10,20.png",
            "params": {
                "collection_concept_id": "C123-TEST",
                "granule_ur": "G123",
                "variables": ["temperature"],
            },
            "timeout": 5,
        },
    ]


def test_assess_collection_compatibility_tries_variables_until_bbox_succeeds():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "variables": {
                        "temperature": {"shape": [10, 10], "dtype": "float32"},
                        "quality": {"shape": [10, 10], "dtype": "uint8"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(500, text="temperature failed"),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is True
    assert result["variables"] == ["quality"]
    assert result["selected_variable_source"] == "fallback_exhaustive"
    assert result["bbox_status_code"] == 200
    assert result["bbox_attempt_count"] == 2
    assert result["bbox_attempts"] == [
        {
            "variables": ["temperature"],
            "selected_variable_source": "fallback_exhaustive",
            "dimension_selectors": [],
            "temporal": None,
            "selected_dimension_source": None,
            "group": None,
            "bbox": [-10.0, -20.0, 10.0, 20.0],
            "status_code": 500,
            "error_snippet": "temperature failed",
            "url": "https://titiler.example.test/xarray/bbox/-10,-20,10,20.png?collection_concept_id=C123-TEST&granule_ur=G123&variables=temperature",
        },
        {
            "variables": ["quality"],
            "selected_variable_source": "fallback_exhaustive",
            "dimension_selectors": [],
            "temporal": None,
            "selected_dimension_source": None,
            "group": None,
            "bbox": [-10.0, -20.0, 10.0, 20.0],
            "status_code": 200,
            "error_snippet": None,
            "url": "https://titiler.example.test/xarray/bbox/-10,-20,10,20.png?collection_concept_id=C123-TEST&granule_ur=G123&variables=quality",
        },
    ]
    assert client.calls[1]["params"]["variables"] == ["temperature"]
    assert client.calls[2]["params"]["variables"] == ["quality"]


def test_assess_collection_compatibility_selects_time_slice_for_multidimensional_xarray():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [
                        {
                            "RangeDateTimes": [
                                {"BeginningDateTime": "1980-01-01T00:00:00.000Z"}
                            ]
                        }
                    ],
                    "dimensions": {"time": 24, "lat": 361, "lon": 576},
                    "coordinates": {
                        "lon": {"size": 576, "dtype": "float64", "min": -180.0, "max": 179.375},
                        "lat": {"size": 361, "dtype": "float64", "min": -90.0, "max": 90.0},
                        "time": {"size": 24, "dtype": "datetime64[ns]", "min": None, "max": None},
                    },
                    "variables": {
                        "BCANGSTR": {"shape": [24, 361, 576], "dtype": "float32"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
            granule_temporal="1980-01-15T01:30:00.000Z",
        )
    )

    assert result["compatible"] is True
    assert result["variables"] == ["BCANGSTR"]
    assert result["dimension_selectors"] == ["time=nearest::{datetime}"]
    assert result["temporal"] == "1980-01-15T01:30:00.000Z"
    assert result["selected_dimension_source"] == "time:granule_temporal_datetime"
    assert result["bbox_url"] == (
        "https://titiler.example.test/xarray/bbox/-10,-20,10,20.png?"
        "collection_concept_id=C123-TEST&granule_ur=G123&variables=BCANGSTR&"
        "sel=time%3Dnearest%3A%3A%7Bdatetime%7D&"
        "temporal=1980-01-15T01%3A30%3A00.000Z"
    )
    assert client.calls[1]["params"]["sel"] == ["time=nearest::{datetime}"]
    assert client.calls[1]["params"]["temporal"] == "1980-01-15T01:30:00.000Z"


def test_assess_collection_compatibility_sends_temporal_when_required_by_links():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [
                        {
                            "RangeDateTimes": [
                                {"BeginningDateTime": "1980-01-01T00:00:00.000Z"}
                            ]
                        }
                    ],
                    "dimensions": {"calib_dim": 8},
                    "coordinates": {},
                    "variables": {
                        "temperature": {"shape": [8], "dtype": "float32"},
                    },
                    "tileable_variables": ["temperature"],
                    "links": [
                        {
                            "href": "https://titiler.example.test/xarray/tiles/{z}/{x}/{y}?collection_concept_id=C123-TEST&variables=temperature&temporal={temporal}"
                        }
                    ],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
            granule_temporal="1980-01-15T01:30:00.000Z",
        )
    )

    assert result["compatible"] is True
    assert result["dimension_selectors"] == []
    assert result["temporal"] == "1980-01-15T01:30:00.000Z"
    assert result["selected_dimension_source"] == "temporal:granule_temporal_datetime"
    assert "sel" not in client.calls[1]["params"]
    assert client.calls[1]["params"]["temporal"] == "1980-01-15T01:30:00.000Z"


def test_assess_collection_compatibility_does_not_select_singleton_time_dimension():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [
                        {
                            "RangeDateTimes": [
                                {"BeginningDateTime": "1980-01-01T00:00:00.000Z"}
                            ]
                        }
                    ],
                    "dimensions": {"time": 1, "lat": 224, "lon": 464},
                    "coordinates": {
                        "time": {"size": 1, "dtype": "datetime64[ns]", "min": None, "max": None},
                    },
                    "variables": {
                        "SWdown": {"shape": [1, 224, 464], "dtype": "float32"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
            granule_temporal="1980-01-15T01:30:00.000Z",
        )
    )

    assert result["compatible"] is True
    assert result["dimension_selectors"] == []
    assert result["temporal"] is None
    assert result["selected_dimension_source"] is None
    assert "sel" not in client.calls[1]["params"]
    assert "temporal" not in client.calls[1]["params"]


def test_assess_collection_compatibility_fetches_granule_temporal_when_missing():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [
                        {
                            "RangeDateTimes": [
                                {"BeginningDateTime": "1980-01-01T00:00:00.000Z"}
                            ]
                        }
                    ],
                    "dimensions": {"time": 24, "lat": 361, "lon": 576},
                    "variables": {
                        "BCANGSTR": {"shape": [24, 361, 576], "dtype": "float32"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(
                200,
                {
                    "items": [
                        {
                            "umm": {
                                "TemporalExtent": {
                                    "RangeDateTime": {
                                        "BeginningDateTime": "2020-01-02T03:04:05.000Z"
                                    }
                                }
                            }
                        }
                    ]
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is True
    assert result["temporal"] == "2020-01-02T03:04:05.000Z"
    assert result["selected_dimension_source"] == "time:granule_temporal_datetime"
    assert client.calls[1]["url"] == "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    assert client.calls[2]["params"]["temporal"] == "2020-01-02T03:04:05.000Z"


def test_assess_collection_compatibility_uses_compatibility_link_variable_first():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "variables": {
                        "temperature": {"shape": [10, 10], "dtype": "float32"},
                        "quality": {"shape": [10, 10], "dtype": "uint8"},
                    },
                    "links": [
                        {
                            "href": "https://tiles.example.test?variables=quality"
                        }
                    ],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is True
    assert result["variables"] == ["quality"]
    assert result["selected_variable_source"] == "compatibility_link"
    assert client.calls[1]["params"]["variables"] == ["quality"]


def test_assess_collection_compatibility_caps_xarray_variable_attempts():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "variables": {
                        "var1": {"shape": [10, 10], "dtype": "float32"},
                        "var2": {"shape": [10, 10], "dtype": "float32"},
                        "var3": {"shape": [10, 10], "dtype": "float32"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(500, text="var1 failed"),
            FakeResponse(500, text="var2 failed"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
            max_bbox_variable_attempts=2,
        )
    )

    assert result["compatible"] is False
    assert result["bbox_attempt_count"] == 2
    assert result["bbox_probe_limited"] is True
    assert [call["params"]["variables"] for call in client.calls[1:]] == [
        ["var1"],
        ["var2"],
    ]


def test_assess_collection_compatibility_treats_204_as_no_content_failure():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "rasterio",
                    "datetime": [],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(204),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is False
    assert result["failure_reason"] == "bbox_request_failed"
    assert result["bbox_status_code"] == 204
    assert result["bbox_error_body"] == "BBox probe returned no rendered content (HTTP 204 No Content)."
    assert result["bbox_attempt_count"] == 1
    assert result["bbox_attempts"][0]["error_snippet"] == result["bbox_error_body"]


def test_assess_collection_compatibility_backs_off_after_large_aoi_error():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "rasterio",
                    "datetime": [],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(400, text="AOI is too large"),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-180, -90, 180, 90),
        )
    )

    assert result["compatible"] is True
    assert result["bbox_status_code"] == 200
    assert result["bbox_attempt_count"] == 2
    assert [attempt["status_code"] for attempt in result["bbox_attempts"]] == [400, 200]
    assert client.calls[1]["url"].endswith("/rasterio/bbox/-180,-90,180,90.png")
    assert client.calls[2]["url"].endswith("/rasterio/bbox/-18,-9,18,9.png")


def test_assess_collection_compatibility_backs_off_after_xarray_503_before_next_variable():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "dimensions": {"time": 1, "lat": 3500, "lon": 7000},
                    "variables": {
                        "temperature": {
                            "shape": [1, 3500, 7000],
                            "dtype": "float32",
                        },
                        "quality": {
                            "shape": [1, 3500, 7000],
                            "dtype": "uint8",
                        },
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(503, text='{"message":"Service Unavailable"}'),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(30, 21, 100, 56),
            granule_temporal="2020-01-01T00:00:00.000Z",
        )
    )

    assert result["compatible"] is True
    assert result["variables"] == ["temperature"]
    assert result["bbox_status_code"] == 200
    assert result["bbox_attempt_count"] == 2
    assert result["probe_bbox"] == (61.5, 36.75, 68.5, 40.25)
    assert [call["params"]["variables"] for call in client.calls[1:]] == [
        ["temperature"],
        ["temperature"],
    ]
    assert client.calls[1]["url"].endswith("/xarray/bbox/30,21,100,56.png")
    assert client.calls[2]["url"].endswith("/xarray/bbox/61.5,36.75,68.5,40.25.png")


def test_assess_collection_compatibility_does_not_back_off_after_xarray_500():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "dimensions": {"number_of_lines": 15087, "pixels_per_line": 1217},
                    "variables": {
                        "Rrs_560": {"shape": [15087, 1217], "dtype": "float32"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(
                500,
                text=(
                    "{\"detail\":\"Couldn't find X and Y spatial coordinates in "
                    "('band', 'number_of_lines', 'pixels_per_line')\"}"
                ),
            ),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-137.40558, -80.0261, 39.70252, 60.09644),
        )
    )

    assert result["compatible"] is False
    assert result["failure_reason"] == "bbox_request_failed"
    assert result["variables"] == ["Rrs_560"]
    assert result["bbox_status_code"] == 500
    assert result["bbox_attempt_count"] == 1
    assert result["probe_bbox"] == (-137.40558, -80.0261, 39.70252, 60.09644)
    assert len(client.calls) == 2


def test_assess_collection_compatibility_backs_off_after_maximum_array_limit_error():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "dimensions": {"number_of_lines": 15087, "pixels_per_line": 1217},
                    "variables": {
                        "Rrs_560": {"shape": [15087, 1217], "dtype": "float32"},
                    },
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(
                500,
                text=(
                    "{\"detail\":\"Maximum array limit 1000000000 reached, "
                    "trying to put DataArray of (1, 3500, 7000) in memory.\"}"
                ),
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test/",
            client=client,
            timeout=5,
            granule_bbox=(-137.40558, -80.0261, 39.70252, 60.09644),
        )
    )

    assert result["compatible"] is True
    assert result["bbox_status_code"] == 200
    assert result["bbox_attempt_count"] == 2
    assert [attempt["status_code"] for attempt in result["bbox_attempts"]] == [500, 200]


def test_assess_collection_compatibility_uses_first_valid_group():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "compatible_groups": ["/bad", "/good"],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "variables": {},
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "variables": {"albedo": {"shape": [10, 10], "dtype": "float32"}},
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is True
    assert result["group"] == "/good"
    assert result["compatible_groups"] == ["/bad", "/good"]
    assert result["tested_groups"] == ["/bad", "/good"]
    assert result["bbox_url"] == "https://titiler.example.test/xarray/bbox/-10,-20,10,20.png?collection_concept_id=C123-TEST&granule_ur=G123&group=%2Fgood&variables=albedo"
    assert client.calls[-1] == {
        "url": "https://titiler.example.test/xarray/bbox/-10,-20,10,20.png",
        "params": {
            "collection_concept_id": "C123-TEST",
            "granule_ur": "G123",
            "group": "/good",
            "variables": ["albedo"],
        },
        "timeout": 60,
    }


def test_assess_collection_compatibility_uses_bbox_for_rasterio_without_variables():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "rasterio",
                    "datetime": [],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is True
    assert result["backend"] == "rasterio"
    assert result["variables"] == []
    assert client.calls[-1]["url"] == "https://titiler.example.test/rasterio/bbox/-10,-20,10,20.png"
    assert "variables" not in client.calls[-1]["params"]


def test_assess_collection_compatibility_fetches_granule_bbox_when_not_provided():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "rasterio",
                    "datetime": [],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(
                200,
                {
                    "items": [
                        {
                            "umm": {
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
                                }
                            }
                        }
                    ]
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
        )
    )

    assert result["compatible"] is True
    assert client.calls[1]["url"] == "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    assert client.calls[1]["params"] == {
        "collection_concept_id": "C123-TEST",
        "granule_ur": "G123",
        "page_size": 1,
    }
    assert client.calls[2]["url"] == "https://titiler.example.test/rasterio/bbox/-10,-20,10,20.png"


def test_assess_collection_compatibility_reports_bbox_failure():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "rasterio",
                    "datetime": [],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(500, text="bbox failed"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is False
    assert result["failure_reason"] == "bbox_request_failed"
    assert result["bbox_status_code"] == 500
    assert result["bbox_error_body"] == "bbox failed"
    assert result["bbox_url"] == "https://titiler.example.test/rasterio/bbox/-10,-20,10,20.png?collection_concept_id=C123-TEST&granule_ur=G123"


def test_assess_collection_compatibility_reports_missing_granule_bbox():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "rasterio",
                    "datetime": [],
                    "granule_ur": "G123",
                },
            ),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
            granule_bbox=(10, -20, -10, 20),
        )
    )

    assert result["compatible"] is False
    assert result["failure_reason"] == "granule_bbox_unavailable"
    assert result["bbox_status_code"] is None
    assert len(client.calls) == 1


def test_assess_collection_compatibility_retries_transient_compatibility_failure():
    client = FakeAsyncClient(
        [
            FakeResponse(503, text='{"message":"Service Unavailable"}'),
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "rasterio",
                    "datetime": [],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(200, b"image"),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
            granule_bbox=(-10, -20, 10, 20),
            compatibility_retry_delays=(0,),
        )
    )

    assert result["compatible"] is True
    assert [call["url"] for call in client.calls[:2]] == [
        "https://titiler.example.test/compatibility",
        "https://titiler.example.test/compatibility",
    ]
    assert client.calls[2]["url"] == "https://titiler.example.test/rasterio/bbox/-10,-20,10,20.png"


def test_assess_collection_compatibility_stops_when_tileable_variables_are_empty():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "tileable_variables": [],
                    "variables": {
                        "temperature": {"shape": [10, 10], "dtype": "float32"},
                    },
                    "incompatible_variables": {
                        "temperature": {
                            "dims": ["grid_lat", "grid_lon"],
                            "reason": "missing recognized x/y dimensions",
                        },
                    },
                    "links": [
                        {
                            "href": "https://titiler.example.test/xarray/tiles/{z}/{x}/{y}?collection_concept_id=C123-TEST&variables=temperature"
                        }
                    ],
                    "granule_ur": "G123",
                },
            ),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
            granule_bbox=(-10, -20, 10, 20),
        )
    )

    assert result["compatible"] is False
    assert result["failure_reason"] == "missing_xy_spatial_coordinates"
    assert (
        result["failure_detail"]
        == "The compatibility endpoint opened the sample asset with the xarray backend, "
        "but tileable_variables was empty, indicating no variables have recognized x/y "
        "spatial coordinates."
    )
    assert len(client.calls) == 1


def test_assess_collection_compatibility_reports_xarray_without_tileable_variables():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "compatible_groups": [],
                    "variables": {},
                    "granule_ur": "G123",
                },
            ),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
        )
    )

    assert result["compatible"] is False
    assert result["failure_reason"] == "xarray_no_tileable_variables"
    assert (
        result["failure_detail"]
        == "The compatibility endpoint opened the sample asset with the xarray backend, "
        "but it returned no tileable variables and no compatible groups to inspect."
    )
    assert result["tested_groups"] == []
    assert len(client.calls) == 1


def test_assess_collection_compatibility_reports_invalid_group_compatibility():
    client = FakeAsyncClient(
        [
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "compatible_groups": ["/bad"],
                    "granule_ur": "G123",
                },
            ),
            FakeResponse(
                200,
                {
                    "concept_id": "C123-TEST",
                    "backend": "xarray",
                    "datetime": [],
                    "variables": {},
                    "granule_ur": "G123",
                },
            ),
        ]
    )

    result = asyncio.run(
        assess_collection_compatibility(
            "C123-TEST",
            "G123",
            titiler_cmr_endpoint="https://titiler.example.test",
            client=client,
        )
    )

    assert result["compatible"] is False
    assert result["failure_reason"] == "xarray_no_tileable_variables"
    assert result["failure_detail"] == (
        "The compatibility endpoint opened the sample asset with the xarray backend, "
        "but neither the root response nor the tested groups exposed any tileable "
        "variables. Tested groups: /bad."
    )
    assert result["tested_groups"] == ["/bad"]
    assert len(client.calls) == 2
