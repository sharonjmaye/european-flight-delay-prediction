import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from flight_pipeline import ValidateRow, CheckDuplicateIDs, BAD

VALID = {'id': 'F001', 'source_year': 2023, 'source_month': '202301', 'expected_duration_min': 100, 'delay_min': 20, 'delayed_15min': 1, 'dof': '2023-01-01'}

def test_valid_and_invalid_rows():
    with BeamTestPipeline() as p:
        rows = p | beam.Create([VALID, {**VALID, 'id': None}, {**VALID, 'delayed_15min': 3}])
        result = rows | beam.ParDo(ValidateRow()).with_outputs(BAD, main='good')
        assert_that(result.good, equal_to([VALID]), label='ValidRows')
        assert_that(result[BAD] | beam.Map(lambda r: r['reason']),
                    equal_to(['missing_id', 'invalid_target']), label='InvalidReasons')


def test_duplicate_flight_ids():

    # Three flight records:
    # F001 appears twice, F002 appears once
    rows = [
        {"id": "F001", "delay_min": 20},
        {"id": "F001", "delay_min": 30},
        {"id": "F002", "delay_min": 10}
    ]

    with BeamTestPipeline() as p:

        # Step 1: Create an Apache Beam collection
        input_rows = (
            p
            | "CreateFlights" >> beam.Create(rows)
        )

        # Step 2: Group flights by their ID
        grouped = (
            input_rows
            | "KeyByFlightID" >> beam.Map(
                lambda row: (str(row["id"]), row)
            )
            | "GroupByFlightID" >> beam.GroupByKey()
        )

        # Step 3: Check for duplicate flight IDs
        checked = (
            grouped
            | "CheckDuplicates" >> beam.ParDo(
                CheckDuplicateIDs()
            ).with_outputs(BAD, main="good")
        )

        # Step 4: Verify only F002 is accepted
        assert_that(
            checked.good,
            equal_to([{"id": "F002", "delay_min": 10}]),
            label="CheckGoodRecords"
        )

        # Step 5: Verify both F001 records are quarantined
        expected_bad = [
            {
                "reason": "duplicate_flight_id",
                "id": "F001",
                "row": {"id": "F001", "delay_min": 20}
            },
            {
                "reason": "duplicate_flight_id",
                "id": "F001",
                "row": {"id": "F001", "delay_min": 30}
            }
        ]

        assert_that(
            checked[BAD],
            equal_to(expected_bad),
            label="CheckDuplicateRecords"
        )
