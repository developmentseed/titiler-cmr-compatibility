import asyncio

import pytest

from titiler_cmr_compatibility import api


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def get(self, url, params, headers, timeout):
        self.calls.append({"url": url, "params": dict(params), "headers": headers, "timeout": timeout})
        return self.responses.pop(0)

    async def post(self, url, data, headers, timeout):
        self.calls.append({"url": url, "data": dict(data), "headers": headers, "timeout": timeout})
        return self.responses.pop(0)


def test_fetch_sample_granule_ur_returns_granule_ur(monkeypatch):
    calls = []

    async def fake_fetch_random_granule_metadata(collection_concept_id, client=None, timeout=30):
        calls.append((collection_concept_id, client, timeout))
        return {"umm": {"GranuleUR": "G123"}}, 42

    monkeypatch.setattr(api, "fetch_random_granule_metadata", fake_fetch_random_granule_metadata)

    assert asyncio.run(api.fetch_sample_granule_ur("C123-TEST", timeout=5)) == "G123"
    assert calls == [("C123-TEST", None, 5)]


def test_fetch_sample_granule_ur_returns_none_when_missing(monkeypatch):
    async def fake_fetch_random_granule_metadata(collection_concept_id, client=None, timeout=30):
        return {"umm": {}}, 42

    monkeypatch.setattr(api, "fetch_random_granule_metadata", fake_fetch_random_granule_metadata)

    assert asyncio.run(api.fetch_sample_granule_ur("C123-TEST")) is None


def test_fetch_random_granule_metadata_uses_async_client(monkeypatch):
    monkeypatch.setattr(api.random, "randint", lambda start, end: 4)
    client = FakeAsyncClient(
        [
            FakeResponse({"hits": 10}),
            FakeResponse({"items": [{"umm": {"GranuleUR": "G123"}}]}),
        ]
    )

    granule, total_granules = asyncio.run(
        api.fetch_random_granule_metadata("C123-TEST", client=client, timeout=7)
    )

    assert granule == {"umm": {"GranuleUR": "G123"}}
    assert total_granules == 10
    assert client.calls == [
        {
            "url": api.GRANULES_SEARCH_URL,
            "params": {"collection_concept_id": "C123-TEST", "page_size": 1},
            "headers": api.CMR_HEADERS,
            "timeout": 7,
        },
        {
            "url": api.GRANULES_SEARCH_URL,
            "params": {"collection_concept_id": "C123-TEST", "page_size": 1, "offset": 4},
            "headers": api.CMR_HEADERS,
            "timeout": 7,
        },
    ]


def test_fetch_granule_by_ur_uses_collection_and_granule_ur():
    client = FakeAsyncClient([FakeResponse({"items": [{"umm": {"GranuleUR": "G123"}}]})])

    granule = asyncio.run(api.fetch_granule_by_ur("C123-TEST", "G123", client=client, timeout=7))

    assert granule == {"umm": {"GranuleUR": "G123"}}
    assert client.calls == [
        {
            "url": api.GRANULES_SEARCH_URL,
            "params": {"collection_concept_id": "C123-TEST", "granule_ur": "G123", "page_size": 1},
            "headers": api.CMR_HEADERS,
            "timeout": 7,
        }
    ]


def test_fetch_eligible_cmr_collections_pages_until_total_hits():
    client = FakeAsyncClient(
        [
            FakeResponse(
                {
                    "hits": 3,
                    "items": [
                        {"meta": {"concept-id": "C1"}},
                        {"meta": {"concept-id": "C2"}},
                    ],
                }
            ),
            FakeResponse({"hits": 3, "items": [{"meta": {"concept-id": "C3"}}]}),
        ]
    )

    collections = asyncio.run(api.fetch_eligible_cmr_collections(page_size=2, client=client, timeout=7))

    assert [collection["meta"]["concept-id"] for collection in collections] == ["C1", "C2", "C3"]
    assert [call["params"]["page_num"] for call in client.calls] == [1, 2]
    assert all(call["params"]["page_size"] == 2 for call in client.calls)


def test_fetch_eligible_cmr_collections_honors_limit_with_one_request():
    client = FakeAsyncClient(
        [
            FakeResponse(
                {
                    "hits": 10,
                    "items": [
                        {"meta": {"concept-id": "C1"}},
                        {"meta": {"concept-id": "C2"}},
                        {"meta": {"concept-id": "C3"}},
                        {"meta": {"concept-id": "C4"}},
                    ],
                }
            )
        ]
    )

    collections = asyncio.run(api.fetch_eligible_cmr_collections(limit=3, page_size=4, client=client))

    assert [collection["meta"]["concept-id"] for collection in collections] == ["C1", "C2", "C3"]
    assert len(client.calls) == 1


def test_fetch_eligible_cmr_collections_skips_missing_concept_ids():
    client = FakeAsyncClient(
        [
            FakeResponse(
                {
                    "hits": 2,
                    "items": [
                        {"meta": {}},
                        {"meta": {"concept-id": "C2"}},
                    ],
                }
            )
        ]
    )

    collections = asyncio.run(api.fetch_eligible_cmr_collections(page_size=2, client=client))

    assert [collection["meta"]["concept-id"] for collection in collections] == ["C2"]


def test_fetch_cmr_collections_by_concept_ids_posts_batches_and_reorders(
    monkeypatch,
):
    monkeypatch.setattr(api, "get_eosdis_shortnames", lambda: ["LPDAAC"])
    client = FakeAsyncClient(
        [
            FakeResponse(
                {
                    "hits": 2,
                    "items": [
                        {"meta": {"concept-id": "C2"}},
                        {"meta": {"concept-id": "C1"}},
                    ],
                }
            ),
            FakeResponse(
                {
                    "hits": 1,
                    "items": [{"meta": {"concept-id": "C3"}}],
                }
            ),
        ]
    )

    collections = asyncio.run(
        api.fetch_cmr_collections_by_concept_ids(
            [" C1 ", "C2", "C1", "", "C3"],
            batch_size=2,
            client=client,
            timeout=7,
        )
    )

    assert [collection["meta"]["concept-id"] for collection in collections] == [
        "C1",
        "C2",
        "C3",
    ]
    assert [call["data"] for call in client.calls] == [
        {
            "page_size": 2,
            "has_granules": True,
            "cloud_hosted": True,
            "provider[]": ["LPDAAC"],
            "concept_id": ["C1", "C2"],
        },
        {
            "page_size": 1,
            "has_granules": True,
            "cloud_hosted": True,
            "provider[]": ["LPDAAC"],
            "concept_id": ["C3"],
        },
    ]
    assert all(call["url"] == api.COLLECTIONS_SEARCH_URL for call in client.calls)
    assert all(call["headers"] == api.CMR_HEADERS for call in client.calls)
    assert all(call["timeout"] == 7 for call in client.calls)


def test_fetch_cmr_collections_by_concept_ids_rejects_missing_ids():
    client = FakeAsyncClient([FakeResponse({"hits": 0, "items": []})])

    with pytest.raises(ValueError, match="C1"):
        asyncio.run(api.fetch_cmr_collections_by_concept_ids(["C1"], client=client))


def test_fetch_cmr_collections_by_concept_ids_rejects_invalid_batch_size():
    with pytest.raises(ValueError, match="batch_size"):
        asyncio.run(api.fetch_cmr_collections_by_concept_ids(["C1"], batch_size=0))
