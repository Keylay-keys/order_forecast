"""Retained evidence projection: identity, reconciliation, missing values, purity."""
from copy import deepcopy
import unittest

from order_forecast.api.accounting_performance import gross_sales_cents


def attempt():
    return dict(route='989262', sha256='a'*64, analysis=dict(
        route='989262', periodStart='2026-08-27', periodEnd='2026-09-02',
        adjustments=[dict(key='salesReturns', cents=-500)],
        checks=[dict(label='Net sales', printedCents=9000, calculatedCents=9000)]),
        _evidence=dict(readerVersion='accounting-pdf-v1', sourceText='PRIVATE PDF TEXT', extracted=dict(
            schemaVersion=1, currency='USD', routeNumber='989262', periodStart='2026-08-27', periodEnd='2026-09-02',
            source=dict(documentId='sha256:'+'a'*64),
            sales=dict(grossSales=100, promotions=-4, networkAllowances=-1, netSales=90, returns=-50))))


class PerformanceProjectionTests(unittest.TestCase):
    def test_gross_is_distinct_from_net_and_summary_returns_are_not_counted_twice(self):
        source = attempt()
        before = deepcopy(source)
        self.assertEqual(gross_sales_cents(source), 10000)
        self.assertEqual(source, before)

    def test_cannot_cross_route_period_or_source_identity(self):
        for key, wrong in [('routeNumber', '123456'), ('periodEnd', '2026-09-09'), ('periodStart', '2026-08-20'), ('currency', 'EUR')]:
            source = attempt()
            source['_evidence']['extracted'][key] = wrong
            self.assertIsNone(gross_sales_cents(source), key)
        source = attempt()
        source['sha256'] = 'b'*64
        self.assertIsNone(gross_sales_cents(source))

    def test_only_exact_independently_reconciled_sales_are_projected(self):
        for field in ['grossSales', 'promotions', 'networkAllowances', 'netSales']:
            source = attempt()
            source['_evidence']['extracted']['sales'][field] += .01
            self.assertIsNone(gross_sales_cents(source), field)
        for field in ['printedCents', 'calculatedCents']:
            source = attempt()
            source['analysis']['checks'][0][field] = None
            self.assertIsNone(gross_sales_cents(source), field)
        source = attempt()
        source['analysis']['checks'] *= 2
        self.assertIsNone(gross_sales_cents(source))
        source = attempt()
        source['analysis']['adjustments'] *= 2
        self.assertIsNone(gross_sales_cents(source))

    def test_old_missing_malformed_and_nonfinite_evidence_remains_unknown(self):
        for evidence in [None, {}, [], {'readerVersion': 'other'}, 'bad']:
            source = attempt()
            source['_evidence'] = evidence
            self.assertIsNone(gross_sales_cents(source))
        for bad in [None, True, '100', float('nan'), float('inf'), 100.001, 1000001]:
            source = attempt()
            source['_evidence']['extracted']['sales']['grossSales'] = bad
            self.assertIsNone(gross_sales_cents(source))


if __name__ == '__main__':
    unittest.main()
