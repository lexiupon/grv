import unittest
from unittest.mock import patch
import purge_deleted as purge


class Salesforce:
    token = 'SECRET-DO-NOT-LOG'
    base = 'https://example.invalid'
    api = '67.0'


class Response:
    def __init__(self, data):
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self, bound):
        return self.data


class PurgeTests(unittest.TestCase):
    def test_no_arbitrary_objects(self):
        with self.assertRaises(SystemExit):
            purge.deleted_ids(Salesforce(), 'Account')

    def test_empty_and_oversize_batch_refused(self):
        for ids in ([], ['a' * 18] * 201):
            with self.assertRaises(SystemExit):
                purge.purge_batch(Salesforce(), ids)

    def test_exact_result_ids_and_success_required(self):
        record_id = 'a' * 18
        for returned_id, success in ((record_id, 'true'), ('b' * 18, 'true'), (record_id, 'false')):
            data = ('<Envelope xmlns="urn:partner.soap.sforce.com"><emptyRecycleBinResponse>'
                    f'<result><id>{returned_id}</id><success>{success}</success></result>'
                    '</emptyRecycleBinResponse></Envelope>').encode()
            with patch.object(purge.urllib.request, 'urlopen', return_value=Response(data)):
                if returned_id == record_id and success == 'true':
                    purge.purge_batch(Salesforce(), [record_id])
                else:
                    with self.assertRaises(SystemExit):
                        purge.purge_batch(Salesforce(), [record_id])


if __name__ == '__main__':
    unittest.main()
