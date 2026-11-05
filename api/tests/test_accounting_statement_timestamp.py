import unittest
from order_forecast.api.accounting_statement_timestamp import printed_issued_at, statement_issued_at


class PrintedIssueTimeTests(unittest.TestCase):
    def test_first_page_header_is_authoritative_and_timezone_free(self):
        self.assertEqual(printed_issued_at('\n   Gruma Corporation      09/14/26 19:25\nBilling Period 9/3/2026 - 9/9/2026\fGruma Corporation 09/14/26 07:25'), '2026-09-14T19:25')
        self.assertEqual(printed_issued_at('Gruma Corporation 02/29/24 00:00'), '2024-02-29T00:00')

    def test_missing_ambiguous_and_invalid_headers_are_not_guessed(self):
        for text in [None, '', 'Gruma Corporation', 'Invoice 09/14/26 19:25', 'Gruma Corporation 02/29/26 19:25', 'Gruma Corporation 09/14/26 24:00', 'Gruma Corporation 09/14/26 19:60', 'Gruma Corporation 09/14/26 07:25 PM', 'Body\nGruma Corporation 09/14/26 19:25', '\fGruma Corporation 09/14/26 19:25']:
            self.assertIsNone(printed_issued_at(text), text)

    def test_old_evidence_is_projected_without_mutation(self):
        source = {'_evidence': {'sourceText': 'Gruma Corporation 09/14/26 19:25'}, 'analysis': {}}
        self.assertEqual(statement_issued_at(source), '2026-09-14T19:25')
        self.assertEqual(source['analysis'], {})
        self.assertIsNone(statement_issued_at({'_evidence': {'sourceText': None}}))
