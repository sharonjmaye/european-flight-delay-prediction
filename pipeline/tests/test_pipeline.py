import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from flight_pipeline import ValidateRow, BAD

VALID = {'id': 'F001', 'source_month': 202301, 'delayed_15min': 1, 'dof': '2023-01-01'}

def test_valid_and_invalid_rows():
    with TestPipeline() as p:
        rows = p | beam.Create([VALID, {**VALID, 'id': None}, {**VALID, 'delayed_15min': 3}])
        result = rows | beam.ParDo(ValidateRow()).with_outputs(BAD, main='good')
        assert_that(result.good, equal_to([VALID]), label='ValidRows')
        assert_that(result[BAD] | beam.Map(lambda r: r['reason']),
                    equal_to(['missing_id', 'invalid_target']), label='InvalidReasons')
