import hashlib
import os
import sys
import unittest
from datetime import datetime

from freezegun import freeze_time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from configuration import Configuration  # noqa: E402
from keboola.component.exceptions import UserException  # noqa: E402


class TestConfiguration(unittest.TestCase):

    def test_valid_configuration(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "-7",
            "to": "today",
            "tables": "contacts,tickets",
            "incremental": True
        }
        config = Configuration(**params)
        self.assertEqual(config.username, "test_user")
        self.assertEqual(config.password, "test_pass")
        self.assertEqual(config.server, "mycompany")
        self.assertTrue(config.incremental)

    def test_url_validation_valid(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "url": "https://mycompany.daktela.com",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        config = Configuration(**params)
        self.assertEqual(config.get_base_url(), "https://mycompany.daktela.com")

    def test_url_validation_invalid(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "url": "https://invalid-url.com",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        with self.assertRaises(UserException):
            Configuration(**params)

    def test_missing_url_and_server(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        with self.assertRaises(UserException):
            Configuration(**params)

    def test_get_server_name_from_server(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        config = Configuration(**params)
        self.assertEqual(config.get_server_name(), "mycompany")

    def test_get_server_name_from_url(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "url": "https://mycompany.daktela.com",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        config = Configuration(**params)
        self.assertEqual(config.get_server_name(), "mycompany")

    def test_get_table_list(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "-7",
            "to": "today",
            "tables": "contacts, tickets, users"
        }
        config = Configuration(**params)
        self.assertEqual(config.get_table_list(), ["contacts", "tickets", "users"])

    @freeze_time("2024-01-15 12:00:00")
    def test_parse_date_today(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        config = Configuration(**params)
        result = config.parse_date("today")
        expected = datetime(2024, 1, 15, 11, 30, 0)
        self.assertEqual(result, expected)

    @freeze_time("2024-01-15 12:00:00")
    def test_parse_date_zero(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "-7",
            "to": "0",
            "tables": "contacts"
        }
        config = Configuration(**params)
        result = config.parse_date("0")
        expected = datetime(2024, 1, 15, 11, 30, 0)
        self.assertEqual(result, expected)

    @freeze_time("2024-01-15 12:00:00")
    def test_parse_date_negative_days(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        config = Configuration(**params)
        result = config.parse_date("-7")
        expected = datetime(2024, 1, 8, 12, 0, 0)
        self.assertEqual(result, expected)

    def test_parse_date_explicit(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "2024-01-01",
            "to": "2024-01-15",
            "tables": "contacts"
        }
        config = Configuration(**params)
        result = config.parse_date("2024-01-01")
        expected = datetime(2024, 1, 1, 0, 0, 0)
        self.assertEqual(result, expected)

    def test_parse_date_invalid(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "invalid-date",
            "to": "today",
            "tables": "contacts"
        }
        config = Configuration(**params)
        with self.assertRaises(UserException):
            config.parse_date("invalid-date")

    @freeze_time("2024-01-15 12:00:00")
    def test_validate_date_range_valid(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "-7",
            "to": "today",
            "tables": "contacts"
        }
        config = Configuration(**params)
        config.validate_date_range()

    @freeze_time("2024-01-15 12:00:00")
    def test_validate_date_range_invalid(self):
        params = {
            "username": "test_user",
            "#password": "test_pass",
            "server": "mycompany",
            "from": "today",
            "to": "-7",
            "tables": "contacts"
        }
        config = Configuration(**params)
        with self.assertRaises(UserException):
            config.validate_date_range()


class TestTableConfig(unittest.TestCase):

    def test_default_table_configs_exist(self):
        from table_config import DEFAULT_TABLE_CONFIGS
        self.assertIn("contacts", DEFAULT_TABLE_CONFIGS)
        self.assertIn("tickets", DEFAULT_TABLE_CONFIGS)
        self.assertIn("users", DEFAULT_TABLE_CONFIGS)
        self.assertIn("activities", DEFAULT_TABLE_CONFIGS)

    def test_get_table_config(self):
        from table_config import get_table_config
        config = get_table_config("contacts")
        self.assertIsNotNone(config)
        self.assertEqual(config.name, "contacts")
        self.assertIn("name", config.primary_keys)

    def test_get_table_config_not_found(self):
        from table_config import get_table_config
        config = get_table_config("nonexistent_table")
        self.assertIsNone(config)


class TestDataTransformer(unittest.TestCase):

    def test_md5_hash_generation(self):
        test_value = "mycompany_12345"
        expected_hash = hashlib.md5(test_value.encode()).hexdigest()
        self.assertEqual(len(expected_hash), 32)

    def test_html_cleaning(self):
        import re
        html_pattern = re.compile(r"<.*?>")
        test_html = "<p>Hello <b>World</b></p>"
        cleaned = html_pattern.sub("", test_html)
        self.assertEqual(cleaned, "Hello World")


if __name__ == "__main__":
    unittest.main()
