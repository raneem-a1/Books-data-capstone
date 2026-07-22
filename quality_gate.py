"""
Capstone Deliverable 5: Quality Gate + Lineage

Success path:
    python quality_gate.py

Failure path:
    INJECT_BAD_ROWS=true python quality_gate.py
"""

import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx
import pandas as pd

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.transport.console import (
    ConsoleConfig,
    ConsoleTransport,
)


# Show OpenLineage JSON events in the Terminal.
logging.basicConfig(level=logging.INFO, format="%(message)s")

PROJECT_DIR = Path(__file__).resolve().parent
RAW_CSV = PROJECT_DIR / "data" / "Books.csv"

JOB_NAMESPACE = "books-capstone"
JOB_NAME = "quality_gate"
PRODUCER_URL = "https://github.com/raneem-a1/Books-data-capstone"

run_id = str(uuid.uuid4())

ol_client = OpenLineageClient(
    transport=ConsoleTransport(ConsoleConfig())
)


def emit(event_state: RunState) -> None:
    """Emit a real OpenLineage lifecycle event."""

    event = RunEvent(
        eventType=event_state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(
            namespace=JOB_NAMESPACE,
            name=JOB_NAME,
        ),
        producer=PRODUCER_URL,
    )

    ol_client.emit(event)


def prepare_clean_baseline(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the original dataset before the quality gate.

    This represents the cleaned Silver-style input that should pass
    validation during the normal success path.
    """

    required_columns = {
        "Title",
        "Author",
        "Genre",
        "Height",
        "Publisher",
    }

    missing_columns = required_columns - set(raw_df.columns)

    if missing_columns:
        raise ValueError(
            f"Dataset is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    df = raw_df.copy()

    # Convert empty or whitespace-only strings to missing values.
    df["Author"] = df["Author"].replace(r"^\s*$", pd.NA, regex=True)
    df["Genre"] = df["Genre"].replace(r"^\s*$", pd.NA, regex=True)

    # Preserve the rows while providing valid baseline values.
    df["Author"] = df["Author"].fillna("Unknown Author")
    df["Genre"] = df["Genre"].fillna("Unknown Genre")

    # Convert Height to numeric.
    df["Height"] = pd.to_numeric(
        df["Height"],
        errors="coerce",
    )

    # Use the valid median for missing/non-numeric Height values.
    valid_heights = df.loc[
        df["Height"].between(0, 400),
        "Height",
    ]

    if valid_heights.empty:
        replacement_height = 0
    else:
        replacement_height = float(valid_heights.median())

    df["Height"] = df["Height"].fillna(replacement_height)

    # Keep baseline Height values inside the accepted domain.
    df["Height"] = df["Height"].clip(
        lower=0,
        upper=400,
    )

    return df


def inject_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Inject invalid rows to prove the failure path."""

    bad_rows = pd.DataFrame(
        [
            {
                "Title": "Missing Author Book",
                "Author": None,
                "Genre": "fiction",
                "Height": 200,
                "Publisher": "Test Publisher",
            },
            {
                "Title": "Negative Height Book",
                "Author": "Test Author",
                "Genre": "fiction",
                "Height": -15,
                "Publisher": "Test Publisher",
            },
            {
                "Title": "Impossibly Tall Book",
                "Author": "Test Author",
                "Genre": "fiction",
                "Height": 9999,
                "Publisher": "Test Publisher",
            },
        ]
    )

    return pd.concat(
        [df, bad_rows],
        ignore_index=True,
    )


def build_expectation_suite(context):
    """Create the real Great Expectations validation rules."""

    suite = gx.core.expectation_suite.ExpectationSuite(
        name=f"books_quality_suite_{run_id}"
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="Author"
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="Genre"
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Height",
            min_value=0,
            max_value=400,
        )
    )

    return context.suites.add(suite)


def display_results(result) -> None:
    """Print readable Great Expectations results."""

    print("=" * 65)
    print("GREAT EXPECTATIONS VALIDATION RESULTS")
    print("=" * 65)

    for validation_result in result.results:
        expectation_name = (
            validation_result.expectation_config.type
        )

        passed = validation_result.success

        unexpected_count = (
            validation_result.result.get(
                "unexpected_count",
                0,
            )
        )

        status = "PASS" if passed else "FAIL"

        print(
            f"[{status}] {expectation_name} "
            f"-- {unexpected_count} violating row(s)"
        )


def main() -> None:
    """Execute the quality gate and emit lineage events."""

    print(f"OpenLineage run_id: {run_id}")
    print(f"Reading data from: {RAW_CSV}\n")

    emit(RunState.START)
    print("[OpenLineage] START event emitted.\n")

    try:
        if not RAW_CSV.exists():
            raise FileNotFoundError(
                f"Dataset not found: {RAW_CSV}"
            )

        raw_df = pd.read_csv(RAW_CSV)

        print(f"Original dataset rows: {len(raw_df)}")
        print(
            "Original missing Authors: "
            f"{raw_df['Author'].isna().sum()}"
        )

        df = prepare_clean_baseline(raw_df)

        print(f"Prepared baseline rows: {len(df)}")
        print(
            "Missing Authors after preparation: "
            f"{df['Author'].isna().sum()}"
        )
        print(
            "Missing Genres after preparation: "
            f"{df['Genre'].isna().sum()}"
        )
        print(
            "Invalid Heights after preparation: "
            f"{(~df['Height'].between(0, 400)).sum()}"
        )

        inject_bad = (
            os.getenv("INJECT_BAD_ROWS", "false")
            .strip()
            .lower()
            == "true"
        )

        if inject_bad:
            df = inject_bad_rows(df)
            print("\nInjected 3 deliberately invalid rows.")
        else:
            print("\nNo deliberately invalid rows were injected.")

        print(f"Rows submitted to quality gate: {len(df)}\n")

        context = gx.get_context()

        data_source = context.data_sources.add_pandas(
            name=f"books_pandas_source_{run_id}"
        )

        data_asset = data_source.add_dataframe_asset(
            name=f"books_asset_{run_id}"
        )

        batch_definition = (
            data_asset.add_batch_definition_whole_dataframe(
                name=f"books_batch_{run_id}"
            )
        )

        suite = build_expectation_suite(context)

        batch = batch_definition.get_batch(
            batch_parameters={
                "dataframe": df,
            }
        )

        result = batch.validate(suite)

        display_results(result)

        if not result.success:
            failed_count = sum(
                not validation_result.success
                for validation_result in result.results
            )

            print(
                f"\nQUALITY GATE FAILED: "
                f"{failed_count} expectation(s) failed."
            )

            emit(RunState.FAIL)
            print("[OpenLineage] FAIL event emitted.")

            # Airflow reads this as a failed task.
            sys.exit(1)

        print(
            "\nQUALITY GATE PASSED: "
            "all expectations succeeded."
        )

        emit(RunState.COMPLETE)
        print("[OpenLineage] COMPLETE event emitted.")

        sys.exit(0)

    except SystemExit:
        raise

    except Exception as error:
        emit(RunState.FAIL)

        print(
            "\n[OpenLineage] FAIL event emitted because "
            f"of an unexpected error: {error}"
        )

        raise


if __name__ == "__main__":
    main()