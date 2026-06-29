import json

from typer.testing import CliRunner

from titiler_cmr_compatibility import cli

runner = CliRunner()


def _json_from_output(output):
    return json.loads(output[output.index("{") :])


def test_assess_collection_command_samples_granule_and_prints_result(monkeypatch):
    calls = []

    async def fake_fetch_random_granule_metadata(collection_concept_id):
        calls.append(("fetch", collection_concept_id))
        return (
            {
                "umm": {
                    "GranuleUR": "G123",
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
                }
            },
            1,
        )

    async def fake_assess_collection_compatibility(
        collection_concept_id,
        granule_ur,
        titiler_cmr_endpoint,
        timeout,
        granule_bbox,
        granule_temporal,
        max_bbox_variable_attempts,
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
                max_bbox_variable_attempts,
            )
        )
        return {
            "collection_concept_id": collection_concept_id,
            "granule_ur": granule_ur,
            "compatible": True,
        }

    monkeypatch.setattr(
        cli, "fetch_random_granule_metadata", fake_fetch_random_granule_metadata
    )
    monkeypatch.setattr(
        cli,
        "assess_collection_compatibility",
        fake_assess_collection_compatibility,
    )

    result = runner.invoke(
        cli.app,
        [
            "assess-collection",
            "C123-TEST",
            "--endpoint",
            "https://titiler.example.test",
            "--timeout",
            "5",
            "--max-bbox-variable-attempts",
            "7",
        ],
    )

    assert result.exit_code == 0
    assert _json_from_output(result.output) == {
        "collection_concept_id": "C123-TEST",
        "granule_ur": "G123",
        "compatible": True,
    }
    assert calls == [
        ("fetch", "C123-TEST"),
        (
            "assess",
            "C123-TEST",
            "G123",
            "https://titiler.example.test",
            5,
            (-10.0, -20.0, 10.0, 20.0),
            None,
            7,
        ),
    ]


def test_assess_collection_command_exits_when_no_granule_ur(monkeypatch):
    async def fake_fetch_random_granule_metadata(collection_concept_id):
        return None, None

    monkeypatch.setattr(
        cli, "fetch_random_granule_metadata", fake_fetch_random_granule_metadata
    )

    result = runner.invoke(cli.app, ["assess-collection", "C123-TEST"])

    assert result.exit_code == 1
    assert "No sample granule UR found" in result.output


def test_tqdm_assessment_progress_stacks_status_lines(monkeypatch):
    bars = []

    class FakeTqdm:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.descriptions = [kwargs.get("desc", "")]
            self.updates = []
            self.closed = False
            bars.append(self)

        def update(self, amount):
            self.updates.append(amount)

        def set_description_str(self, description, refresh):
            self.descriptions.append(description)

        def close(self):
            self.closed = True

    monkeypatch.setattr(cli, "tqdm", FakeTqdm)

    progress = cli._TqdmAssessmentProgress(enabled=True, max_in_flight=2)
    progress(
        cli.AssessmentProgressEvent(
            name="batch_started",
            total=4,
            compatible=0,
            incompatible=0,
            errors=0,
        )
    )
    progress(
        cli.AssessmentProgressEvent(
            name="collection_started",
            total=4,
            compatible=1,
            incompatible=2,
            errors=1,
            in_flight=("C123-TEST", "C456-TEST", "C789-TEST"),
        )
    )
    progress(
        cli.AssessmentProgressEvent(
            name="collection_finished",
            total=4,
            compatible=1,
            incompatible=3,
            errors=1,
            in_flight=(),
        )
    )

    assert len(bars) == 8
    assert bars[0].kwargs["desc"] == "Assessing collections"
    assert bars[0].kwargs["position"] == 0
    assert bars[0].updates == [1]
    assert bars[1].descriptions[-2] == "compatible: 1"
    assert bars[2].descriptions[-2] == "incompatible: 2"
    assert bars[3].descriptions[-2] == "errors: 1"
    assert bars[4].descriptions[-2] == "in flight:"
    assert bars[5].descriptions[-2] == "  C123-TEST"
    assert bars[6].descriptions[-2] == "  C456-TEST"
    assert bars[7].descriptions[-2] == "  +1 more"
    assert bars[4].descriptions[-1] == "in flight: none"

    progress.close()

    assert all(bar.closed for bar in bars)


def test_assess_collections_command_wires_batch_arguments(monkeypatch, tmp_path):
    calls = []
    output = tmp_path / "results.parquet"

    async def fake_run_batch_assessment(**kwargs):
        calls.append(kwargs)
        kwargs["progress"]("Wrote 1 assessment rows to results.parquet.")
        return [{"collection_concept_id": "C123-TEST", "tiling_compatible": True}]

    monkeypatch.setattr(cli, "run_batch_assessment", fake_run_batch_assessment)

    result = runner.invoke(
        cli.app,
        [
            "assess-collections",
            "--limit",
            "2",
            "--output",
            str(output),
            "--concurrency",
            "3",
            "--endpoint",
            "https://titiler.example.test",
            "--timeout",
            "5",
            "--max-bbox-variable-attempts",
            "12",
        ],
    )

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["limit"] == 2
    assert calls[0]["output_path"] == output
    assert calls[0]["titiler_cmr_endpoint"] == "https://titiler.example.test"
    assert calls[0]["timeout"] == 5
    assert calls[0]["concurrency"] == 3
    assert calls[0]["max_bbox_variable_attempts"] == 12
    assert calls[0]["progress"] == cli.typer.echo
    assert callable(calls[0]["progress_callback"])
    assert calls[0]["progress_callback"].max_in_flight == 3
    assert result.output.count("Wrote 1 assessment rows") == 1


def test_assess_collections_command_wires_explicit_collection_ids(monkeypatch, tmp_path):
    calls = []
    ids_file = tmp_path / "collection-ids.txt"
    ids_file.write_text("C456-TEST\n\n")

    async def fake_run_batch_assessment(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(cli, "run_batch_assessment", fake_run_batch_assessment)

    result = runner.invoke(
        cli.app,
        [
            "assess-collections",
            "--collection-concept-id",
            "C123-TEST",
            "--collection-concept-ids-file",
            str(ids_file),
            "--output",
            str(tmp_path / "results.parquet"),
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["collection_concept_ids"] == ["C123-TEST", "C456-TEST"]


def test_assess_collections_command_reads_collection_ids_from_stdin(
    monkeypatch, tmp_path
):
    calls = []

    async def fake_run_batch_assessment(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(cli, "run_batch_assessment", fake_run_batch_assessment)

    result = runner.invoke(
        cli.app,
        [
            "assess-collections",
            "--collection-concept-ids-file",
            "-",
            "--output",
            str(tmp_path / "results.parquet"),
        ],
        input="C123-TEST\nC456-TEST\n",
    )

    assert result.exit_code == 0
    assert calls[0]["collection_concept_ids"] == ["C123-TEST", "C456-TEST"]


def test_assess_collections_command_rejects_limit_with_explicit_collection_ids(tmp_path):
    result = runner.invoke(
        cli.app,
        [
            "assess-collections",
            "--limit",
            "2",
            "--collection-concept-id",
            "C123-TEST",
            "--output",
            str(tmp_path / "results.parquet"),
        ],
    )

    assert result.exit_code == 1
    assert "--limit cannot be combined" in result.output


def test_assess_collections_command_passes_no_limit_as_all_collections(
    monkeypatch, tmp_path
):
    calls = []

    async def fake_run_batch_assessment(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(cli, "run_batch_assessment", fake_run_batch_assessment)

    result = runner.invoke(
        cli.app, ["assess-collections", "--output", str(tmp_path / "results.parquet")]
    )

    assert result.exit_code == 0
    assert calls[0]["limit"] is None


def test_assess_collections_command_rejects_invalid_concurrency(tmp_path):
    result = runner.invoke(
        cli.app,
        [
            "assess-collections",
            "--output",
            str(tmp_path / "results.parquet"),
            "--concurrency",
            "0",
        ],
    )

    assert result.exit_code != 0
    assert "Invalid value" in result.output


def test_assess_collections_command_exits_on_run_level_failure(monkeypatch, tmp_path):
    async def fake_run_batch_assessment(**kwargs):
        raise RuntimeError("discovery failed")

    monkeypatch.setattr(cli, "run_batch_assessment", fake_run_batch_assessment)

    result = runner.invoke(
        cli.app, ["assess-collections", "--output", str(tmp_path / "results.parquet")]
    )

    assert result.exit_code == 1
    assert "Batch assessment failed: discovery failed" in result.output
