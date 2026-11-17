"""Lecture 6-style Beam pipeline, stage 1: read, validate, quarantine.

This is a deliberately limited starter, NOT the complete ML preparation pipeline.
"""
import argparse
import json
from pathlib import Path
import apache_beam as beam
import pyarrow.parquet as pq
import glob
import apache_beam as beam
import math
from datetime import datetime
import pandas as pd

BAD = 'bad'
REQUIRED = ('id', 'source_year', 'source_month', 'expected_duration_min', 'delay_min', 'delayed_15min', 'dof')

def is_missing(value):
    """Check for None, NaN, NaT, or blank strings."""

    # Check for None
    if value is None:
        return True

    # Check for blank strings (including spaces)
    if isinstance(value, str) and value.strip() == "":
        return True

    # Check for pandas missing values (NaN, NaT, pd.NA)
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False

class RowError(ValueError):
    """A record violates the flight data contract."""

class ReadParquetFile(beam.DoFn):
    def process(self, filename):
        for batch in pq.ParquetFile(filename).iter_batches(batch_size=10000):
            for row in batch.to_pylist():
                yield row

class ValidateRow(beam.DoFn):

    def process(self, row):
        try:
            # 1. Check required columns
            missing = [c for c in REQUIRED if c not in row]
            if missing:
                raise RowError(
                    "missing_columns:" + ",".join(missing)
                )

            # 2. Check unique flight identifier is populated
            if is_missing(row["id"]):
                raise RowError("missing_id")

            # 3. Check source year
            if is_missing(row["source_year"]):
                raise RowError("missing_source_year")

            try:
                year = int(row["source_year"])
                if float(row["source_year"]) != year:
                    raise ValueError()
            except (ValueError, TypeError, OverflowError):
                raise RowError("invalid_source_year")

            if year not in (2023, 2024, 2025):
                raise RowError("invalid_source_year")

            # 4. Check source month
            if is_missing(row["source_month"]):
                raise RowError("missing_source_month")

            month = str(row["source_month"])

            if (
                len(month) != 6
                or not month.isdigit()
                or int(month[:4]) != year
                or not 1 <= int(month[4:]) <= 12
            ):
                raise RowError("invalid_source_month")

            # 5. Check binary target
            target = row["delayed_15min"]

            if is_missing(target) or (
                target not in (0, 1, False, True)
            ):
                raise RowError("invalid_target")

            # 6. Check flight date
            if is_missing(row["dof"]):
                raise RowError("missing_flight_date")

            try:
                flight_date = pd.to_datetime(
                    row["dof"], errors="raise"
                )
            except (ValueError, TypeError, OverflowError):
                raise RowError("invalid_flight_date")

            if pd.isna(flight_date):
                raise RowError("missing_flight_date")

            if flight_date.year != year:
                raise RowError("flight_year_mismatch")

            if flight_date.strftime("%Y%m") != month:
                raise RowError("flight_month_mismatch")

            # 7. Check expected duration
            duration = row["expected_duration_min"]
            if is_missing(duration):
                raise RowError("missing_expected_duration")

            try:
                duration = float(duration)
            except (ValueError, TypeError, OverflowError):
                raise RowError("invalid_expected_duration")

            if not math.isfinite(duration) or duration <= 0:
                raise RowError("invalid_expected_duration")

            # 8. Check target consistency against actual delay
            delay = row["delay_min"]
            if not is_missing(delay):
                try:
                    delay = float(delay)
                except (ValueError, TypeError, OverflowError):
                    raise RowError("invalid_delay")

                if not math.isfinite(delay):
                    raise RowError("invalid_delay")

                expected_target = int(delay > 15)

                if int(target) != expected_target:
                    raise RowError("target_delay_mismatch")

            # All checks passed
            yield row

        except RowError as error:
            yield beam.pvalue.TaggedOutput(
                BAD,
                {
                    "reason": str(error),
                    "id": str(row.get("id")),
                    "row": row
                }
            )


class CheckDuplicateIDs(beam.DoFn):
    """Quarantine all records sharing a duplicate flight ID."""

    def process(self, element):
        flight_id, rows = element
        rows = list(rows)

        if len(rows) == 1:
            # Unique flight ID: retain the record
            yield rows[0]
        else:
            # Duplicate flight ID: quarantine every record
            for row in rows:
                yield beam.pvalue.TaggedOutput(
                    BAD,
                    {
                        "reason": "duplicate_flight_id",
                        "id": str(flight_id),
                        "row": row
                    }
                )

def as_json(row):
    return json.dumps(row, default=str, ensure_ascii=False)

def build(pipeline, input_pattern, output_prefix):
    """Build the Apache Beam flight validation pipeline."""

    # Step 1: Find Parquet files
    files = (
        pipeline
        | 'FindParquetFiles' >> beam.Create(
            sorted(glob.glob(str(input_pattern)))
        )
    )

    # Step 2: Read flight records from those files
    rows = (
        files
        | 'ReadRows' >> beam.ParDo(ReadParquetFile())
    )

    # Step 3: Validate each flight record
    checked = (
        rows
        | 'Validate' >> beam.ParDo(ValidateRow()).with_outputs(
            BAD, main='good'
        )
    )

    # Step 4: Group valid records by flight ID
    grouped = (
        checked.good
        | "KeyByFlightID" >> beam.Map(
            lambda row: (str(row["id"]), row)
        )
        | "GroupByFlightID" >> beam.GroupByKey()
    )

    # Identify duplicate flight IDs
    duplicates_checked = (
        grouped
        | "CheckDuplicateIDs" >> beam.ParDo(
            CheckDuplicateIDs()
        ).with_outputs(BAD, main="good")
    )

    # Combine invalid rows from both validation stages
    all_bad = (
        (checked[BAD], duplicates_checked[BAD])
        | "CombineBadRecords" >> beam.Flatten()
    )

    # Write unique, valid records
    _ = (
        duplicates_checked.good
        | "GoodToJSON" >> beam.Map(as_json)
        | "WriteGood" >> beam.io.WriteToText(
            output_prefix + "_good",
            file_name_suffix=".jsonl"
        )
    )

    # Write all quarantined records
    _ = (
        all_bad
        | "BadToJSON" >> beam.Map(as_json)
        | "WriteBad" >> beam.io.WriteToText(
            output_prefix + "_bad",
            file_name_suffix=".jsonl"
        )
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_glob', required=True, help='Relative glob for local Parquet files')
    parser.add_argument('--output_prefix', required=True)
    args = parser.parse_args()
    if not list(Path().glob(args.input_glob)):
        parser.error('No input Parquet files matched --input_glob')
    Path(args.output_prefix).parent.mkdir(parents=True, exist_ok=True)
    with beam.Pipeline() as pipeline:
        build(pipeline, args.input_glob, args.output_prefix)

if __name__ == '__main__':
    main()
