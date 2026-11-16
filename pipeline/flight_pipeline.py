"""Lecture 6-style Beam pipeline, stage 1: read, validate, quarantine.

This is a deliberately limited starter, NOT the complete ML preparation pipeline.
"""
import argparse
import json
from pathlib import Path
import apache_beam as beam
import pyarrow.parquet as pq

BAD = 'bad'
REQUIRED = ('id', 'source_month', 'delayed_15min', 'dof')

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
            missing = [c for c in REQUIRED if c not in row]
            if missing:
                raise RowError('missing_columns:' + ','.join(missing))
            if row['id'] is None or str(row['id']).strip() == '':
                raise RowError('missing_id')
            if row['source_month'] is None:
                raise RowError('missing_source_month')
            if row['delayed_15min'] not in (0, 1, False, True):
                raise RowError('invalid_target')
            if row['dof'] is None or str(row['dof']).strip() == '':
                raise RowError('missing_flight_date')
            yield row
        except RowError as error:
            yield beam.pvalue.TaggedOutput(BAD, {'reason': str(error), 'id': str(row.get('id')), 'row': row})

def as_json(row):
    return json.dumps(row, default=str, ensure_ascii=False)

def build(pipeline, input_pattern, output_prefix):
    """Named stages analogous to the lecture's build() function."""
    files = pipeline | 'FindParquetFiles' >> beam.Create(sorted(str(p) for p in Path().glob(input_pattern)))
    rows = files | 'ReadRows' >> beam.ParDo(ReadParquetFile())
    checked = rows | 'Validate' >> beam.ParDo(ValidateRow()).with_outputs(BAD, main='good')
    _ = (checked.good | 'GoodToJSON' >> beam.Map(as_json)
         | 'WriteGood' >> beam.io.WriteToText(output_prefix + '_good', file_name_suffix='.jsonl'))
    _ = (checked[BAD] | 'BadToJSON' >> beam.Map(as_json)
         | 'WriteBad' >> beam.io.WriteToText(output_prefix + '_bad', file_name_suffix='.jsonl'))
    return checked

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
