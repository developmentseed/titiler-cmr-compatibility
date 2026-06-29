"""Command-line interface for API-first collection compatibility assessment."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
from tqdm import tqdm

from .api import fetch_random_granule_metadata
from .assessment import (
    DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS,
    DEV_TITILER_CMR_ENDPOINT,
    assess_collection_compatibility,
)
from .assessment_runs import (
    AssessmentProgressEvent,
    DEFAULT_CONCURRENCY,
    run_batch_assessment,
)
from .umm_helpers import parse_bounds_from_spatial, parse_temporal

logger = logging.getLogger(__name__)

app = typer.Typer(
    no_args_is_help=True,
    help="Assess NASA CMR collections against a TiTiler-CMR deployment.",
)


@app.callback()
def cli() -> None:
    """Assess NASA CMR collections against a TiTiler-CMR deployment."""


def _json_default(value: Any) -> str:
    return str(value)


def _configure_logging(debug: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if debug else logging.WARNING, force=True)


def _read_collection_concept_ids(path: Path) -> list[str]:
    if str(path) == "-":
        content = sys.stdin.read()
    else:
        content = path.read_text()
    return [line.strip() for line in content.splitlines() if line.strip()]


class _TqdmAssessmentProgress:
    """Render structured assessment progress events with tqdm."""

    def __init__(self, enabled: bool, max_in_flight: int = 3) -> None:
        self.enabled = enabled
        self.max_in_flight = max_in_flight
        self.bar: tqdm | None = None
        self.status_lines: list[tqdm] = []

    def __call__(self, event: AssessmentProgressEvent) -> None:
        if not self.enabled:
            return
        if event.name == "batch_started":
            self._start(event)
            return
        if self.bar is None:
            return
        if event.name == "collection_finished":
            self.bar.update(1)
            self._refresh_status(event)
            return
        if event.name == "collection_started":
            self._refresh_status(event)
            return
        if event.name == "batch_completed":
            self._refresh_status(event)
            self.close()

    def close(self) -> None:
        for status_line in reversed(self.status_lines):
            status_line.close()
        self.status_lines = []
        if self.bar is not None:
            self.bar.close()
            self.bar = None

    def _start(self, event: AssessmentProgressEvent) -> None:
        self.close()
        self.bar = tqdm(
            total=event.total,
            desc="Assessing collections",
            unit="collection",
            dynamic_ncols=True,
            file=sys.stderr,
            position=0,
        )
        self.status_lines = [
            tqdm(
                total=0,
                bar_format="{desc}",
                dynamic_ncols=True,
                file=sys.stderr,
                leave=False,
                position=position,
            )
            for position in range(1, self._status_line_count + 1)
        ]
        self._refresh_status(event)

    @property
    def _status_line_count(self) -> int:
        return self.max_in_flight + 5

    def _refresh_status(self, event: AssessmentProgressEvent) -> None:
        for status_line, description in zip(
            self.status_lines, self._status_descriptions(event), strict=True
        ):
            status_line.set_description_str(
                self._fit_status_line(description), refresh=True
            )

    def _status_descriptions(self, event: AssessmentProgressEvent) -> list[str]:
        lines = [
            f"compatible: {event.compatible or 0}",
            f"incompatible: {event.incompatible or 0}",
            f"errors: {event.errors or 0}",
        ]
        in_flight = event.in_flight[: self.max_in_flight]
        remaining = len(event.in_flight) - len(in_flight)
        if event.in_flight:
            lines.append("in flight:")
            lines.extend(f"  {collection_id}" for collection_id in in_flight)
            lines.extend("" for _ in range(self.max_in_flight - len(in_flight)))
            lines.append(f"  +{remaining} more" if remaining else "")
            return lines

        lines.append("in flight: none")
        lines.extend("" for _ in range(self.max_in_flight + 1))
        return lines

    def _fit_status_line(self, description: str) -> str:
        terminal_width = shutil.get_terminal_size(fallback=(80, 20)).columns
        max_width = max(1, terminal_width - 1)
        if len(description) <= max_width:
            return description
        if max_width <= 3:
            return description[:max_width]
        return f"{description[: max_width - 3]}..."


@app.command("assess-collection")
def assess_collection(
    collection_concept_id: Annotated[
        str,
        typer.Argument(help="CMR collection concept ID to assess."),
    ],
    titiler_cmr_endpoint: Annotated[
        str,
        typer.Option(
            "--endpoint",
            "-e",
            help="TiTiler-CMR endpoint to call.",
        ),
    ] = DEV_TITILER_CMR_ENDPOINT,
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            min=1,
            help="Timeout in seconds for TiTiler-CMR requests.",
        ),
    ] = 60,
    max_bbox_variable_attempts: Annotated[
        int,
        typer.Option(
            "--max-bbox-variable-attempts",
            min=1,
            help="Maximum xarray variables to try per bbox probe location.",
        ),
    ] = DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS,
    debug: Annotated[
        bool,
        typer.Option("--debug", help="Enable debug logging."),
    ] = False,
) -> None:
    """Assess one collection by sampling a granule and probing TiTiler-CMR."""
    _configure_logging(debug)

    typer.echo(f"Fetching sample granule for {collection_concept_id}...", err=True)
    try:
        granule, _ = asyncio.run(fetch_random_granule_metadata(collection_concept_id))
    except Exception as exc:
        if debug:
            logger.exception("Failed to fetch a sample granule")
        typer.secho(
            f"Failed to fetch a sample granule: {exc}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(1) from exc

    granule_ur = granule.get("umm", {}).get("GranuleUR") if granule else None
    granule_bbox = (
        parse_bounds_from_spatial(granule.get("umm", {})) if granule else None
    )
    granule_temporal = None
    if granule:
        granule_temporal = next(
            (value for value in parse_temporal(granule.get("umm", {})) if value), None
        )

    if not granule_ur:
        typer.secho(
            f"No sample granule UR found for collection {collection_concept_id}.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)

    typer.echo(f"Assessing granule UR {granule_ur}...", err=True)
    try:
        result = asyncio.run(
            assess_collection_compatibility(
                collection_concept_id=collection_concept_id,
                granule_ur=granule_ur,
                titiler_cmr_endpoint=titiler_cmr_endpoint,
                timeout=timeout,
                granule_bbox=granule_bbox,
                granule_temporal=granule_temporal,
                max_bbox_variable_attempts=max_bbox_variable_attempts,
            )
        )
    except Exception as exc:
        if debug:
            logger.exception("Compatibility assessment failed")
        typer.secho(
            f"Compatibility assessment failed: {exc}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(1) from exc

    typer.echo(json.dumps(result, indent=2, sort_keys=True, default=_json_default))


@app.command("assess-collections")
def assess_collections(
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            "-n",
            min=1,
            help="Maximum number of eligible CMR collections to assess. Omit to assess all matching collections.",
        ),
    ] = None,
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Destination parquet file path.",
        ),
    ] = Path("assessment-results.parquet"),
    titiler_cmr_endpoint: Annotated[
        str,
        typer.Option(
            "--endpoint",
            "-e",
            help="TiTiler-CMR endpoint to call.",
        ),
    ] = DEV_TITILER_CMR_ENDPOINT,
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            min=1,
            help="Timeout in seconds for CMR and TiTiler-CMR requests.",
        ),
    ] = 60,
    concurrency: Annotated[
        int,
        typer.Option(
            "--concurrency",
            min=1,
            help="Maximum number of collection assessments to run concurrently.",
        ),
    ] = DEFAULT_CONCURRENCY,
    progress: Annotated[
        bool,
        typer.Option(
            "--progress/--no-progress",
            help="Show a live tqdm progress bar when running in a terminal.",
        ),
    ] = True,
    max_bbox_variable_attempts: Annotated[
        int,
        typer.Option(
            "--max-bbox-variable-attempts",
            min=1,
            help="Maximum xarray variables to try per bbox probe location.",
        ),
    ] = DEFAULT_MAX_BBOX_VARIABLE_ATTEMPTS,
    collection_concept_id: Annotated[
        list[str] | None,
        typer.Option(
            "--collection-concept-id",
            help="Explicit CMR collection concept ID to assess. Repeat for multiple collections.",
        ),
    ] = None,
    collection_concept_ids_file: Annotated[
        Path | None,
        typer.Option(
            "--collection-concept-ids-file",
            help="Path to newline-delimited collection concept IDs. Use '-' to read IDs from stdin.",
        ),
    ] = None,
    debug: Annotated[
        bool,
        typer.Option("--debug", help="Enable debug logging."),
    ] = False,
) -> None:
    """Assess eligible collections concurrently and write parquet results."""
    _configure_logging(debug)
    collection_concept_ids = list(collection_concept_id or [])
    if collection_concept_ids_file is not None:
        collection_concept_ids.extend(_read_collection_concept_ids(collection_concept_ids_file))
    if collection_concept_ids and limit is not None:
        typer.secho(
            "--limit cannot be combined with explicit collection concept IDs.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)

    progress_display = _TqdmAssessmentProgress(
        enabled=progress and sys.stderr.isatty(), max_in_flight=concurrency
    )

    try:
        asyncio.run(
            run_batch_assessment(
                limit=limit,
                collection_concept_ids=collection_concept_ids or None,
                output_path=output,
                titiler_cmr_endpoint=titiler_cmr_endpoint,
                timeout=timeout,
                concurrency=concurrency,
                progress=typer.echo,
                progress_callback=progress_display,
                max_bbox_variable_attempts=max_bbox_variable_attempts,
            )
        )
    except Exception as exc:
        progress_display.close()
        if debug:
            logger.exception("Batch assessment failed")
        typer.secho(f"Batch assessment failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc

    progress_display.close()


def main() -> None:
    """Run the Typer application."""
    app()


if __name__ == "__main__":
    main()
