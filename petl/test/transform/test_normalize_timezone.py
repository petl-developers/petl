import unittest
from petl.transform.normalize_timezone import normalize_timezone

class TestNormalizeTimezone(unittest.TestCase):

    def test_basic_conversion(self):
        input_data = [
            {'timestamp': '2023-12-01T10:00:00', 'timezone': 'America/New_York'},
            {'timestamp': '2023-12-01T15:00:00', 'timezone': 'Europe/London'}
        ]
        result = list(normalize_timezone(input_data))
        self.assertEqual(result[0]['timestamp_utc'], '2023-12-01T15:00:00+00:00')
        self.assertEqual(result[1]['timestamp_utc'], '2023-12-01T15:00:00+00:00')
        self.assertEqual(result[0]['timezone_original'], 'America/New_York')

    def test_invalid_timezone(self):
        input_data = [{'timestamp': '2023-12-01T10:00:00', 'timezone': 'Invalid/Zone'}]
        with self.assertRaises(ValueError):
            list(normalize_timezone(input_data))

    def test_missing_timestamp(self):
        input_data = [{'timezone': 'UTC'}]
        with self.assertRaises(ValueError):
            list(normalize_timezone(input_data))

if __name__ == '__main__':
    unittest.main()
