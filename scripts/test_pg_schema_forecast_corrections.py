import unittest

from order_forecast.scripts import pg_schema


class _RecordingCursor:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql: str, _params=None) -> None:
        self.statements.append(" ".join(sql.split()).lower())


class ForecastCorrectionSchemaMigrationTests(unittest.TestCase):
    def test_existing_table_adds_prediction_source_idempotently(self):
        cursor = _RecordingCursor()

        pg_schema._create_order_tables(cursor)

        self.assertTrue(any(
            statement.startswith("alter table forecast_corrections")
            and "add column if not exists prediction_source varchar(50)" in statement
            for statement in cursor.statements
        ))
        self.assertTrue(any(
            statement.startswith("update forecast_corrections")
            and "set prediction_source = 'legacy_unknown'" in statement
            and "prediction_source is null" in statement
            for statement in cursor.statements
        ))


if __name__ == "__main__":
    unittest.main()
