"""Offline regression checks; run python3 -m unittest -v test_fixture."""
import csv
import io
import json
import unittest
from decimal import Decimal
from unittest.mock import patch

import generate as g
import provision as p


class FixtureTests(unittest.TestCase):
    def test_input_and_stored_values_are_separate(self):
        self.assertEqual(g.grvfix_values(1)['GrvText__c'], '')
        self.assertIsNone(g.grvfix_stored_values(1)['GrvText__c'])
        self.assertIsNone(g.grvfix_values(2)['GrvBool__c'])
        self.assertIs(g.grvfix_stored_values(2)['GrvBool__c'], False)
        self.assertEqual(g.grvfix_stored_values(2)['GrvFormula__c'], '-1:F')
        self.assertEqual(g.grvfix_values(13)['GrvDateTime__c'], '1970-01-01T00:00:00.781Z')
        self.assertEqual(g.grvfix_stored_values(13)['GrvDateTime__c'], '1970-01-01T00:00:00.000Z')

    def test_csv_excludes_formula_and_uses_lf(self):
        manifest, data, related = g.build_manifest()
        self.assertNotIn(b'\r\n', data)
        header = next(csv.reader(io.StringIO(data.decode())))
        self.assertNotIn('GrvFormula__c', header)
        self.assertEqual(manifest['objects']['GrvFix__c']['aggregates']['bool_all'],
                         {'False': 666, 'True': 334})
        self.assertEqual(manifest['objects']['GrvFix__c']['aggregates']['nulls_all']['GrvText__c'], 250)
        self.assertEqual(manifest['selections']['base']['row_count'], 900)
        self.assertEqual(manifest['objects']['GrvFix__c']['row_count'], 1000)
        self.assertEqual(manifest['objects']['GrvFixRel__c']['row_count'], 10)
        self.assertEqual(manifest['manifest_version'], 3)
        self.assertEqual(len(list(csv.reader(io.StringIO(related.decode())))), 11)

    def test_strict_readback(self):
        expected = {'GrvText__c': None, 'GrvBool__c': False,
                    'GrvDecimal__c': '99999999.1250000000',
                    'GrvDateTime__c': '1970-01-01T00:00:00.000Z'}
        actual = dict(expected, GrvDecimal__c=Decimal('99999999.125'),
                      GrvDateTime__c='1970-01-01T00:00:00.000+0000')
        self.assertEqual(p.row_errors(expected, actual), [])
        for field, value in [('GrvText__c', ''), ('GrvBool__c', None),
                             ('GrvDecimal__c', Decimal('99999999.12499999')),
                             ('GrvDateTime__c', None),
                             ('GrvDateTime__c', '1970-01-01T00:00:00.001Z')]:
            self.assertTrue(p.row_errors(expected, dict(actual, **{field: value})))
        self.assertTrue(p.row_errors({'GrvText__c': None}, {}))

    def test_assignment_is_idempotent(self):
        with patch.object(p, 'sf_json', side_effect=[{'username': 'fixture@test'}, {'records': [{'Id': 'x'}]}]) as mock:
            p.assign_access('fixture')
            self.assertEqual(mock.call_count, 2)
            self.assertIn('PermissionSetAssignment', mock.call_args[0][0][-1])
        with patch.object(p, 'sf_json', side_effect=[{'username': 'fixture@test'}, {'records': []},
                                                   {'successes': [{'name': 'fixture@test'}], 'failures': []}]) as mock:
            p.assign_access('fixture')
            self.assertEqual(mock.call_args[0][0][:3], ['org', 'assign', 'permset'])

    def test_capacity_fails_before_mutation(self):
        sf = object.__new__(p.Salesforce)
        with patch.object(sf, 'resource', return_value={'DataStorageMB': {'Max': 1, 'Remaining': 1}}):
            with self.assertRaisesRegex(SystemExit, 'CAPACITY CHECK FAILED'):
                sf.check_capacity()

    def test_relative_page_url_and_exact_decimal(self):
        sf = object.__new__(p.Salesforce)
        sf.base = 'https://example.my.salesforce.com'
        sf.api = '67.0'
        with patch.object(sf, 'get', side_effect=[
            {'records': [{'Id': 'a'}], 'nextRecordsUrl': '/services/data/v67.0/query/locator-1'},
            {'records': [{'Id': 'b'}]}]) as get:
            self.assertEqual(sf.query('SELECT Id FROM GrvFix__c'), [{'Id': 'a'}, {'Id': 'b'}])
            self.assertEqual(sf.pages, [1, 1])
            self.assertEqual(get.call_args[0][0], '/services/data/v67.0/query/locator-1')
        self.assertEqual(json.loads('0.1234567891', parse_float=Decimal), Decimal('0.1234567891'))


if __name__ == '__main__':
    unittest.main()
