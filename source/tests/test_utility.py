import unittest
from utility import prepare_dictionaries_from_blended_parameter


class TestUtility(unittest.TestCase):
    def test_prepare_dictionaries_basic(self):
        """Test basic functionality with table configuration"""
        blended_parameter = "table1:col1,col2:key1;table2:*:key2"
        keys = ["name", "columns", "sort_key"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)

        expected = [
            {"name": "table1", "columns": ["col1", "col2"], "sort_key": "key1"},
            {"name": "table2", "columns": ["*"], "sort_key": "key2"},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_empty_input(self):
        """Test with empty input string"""
        blended_parameter = ""
        keys = ["name", "columns", "sort_key"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)
        self.assertEqual(result, [])

    def test_prepare_dictionaries_missing_values(self):
        """Test with missing values which should be None"""
        blended_parameter = "table1:col1;table2:col2,col3:key2:extra"
        keys = ["name", "columns", "sort_key", "dist_key"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)

        expected = [
            {"name": "table1", "columns": ["col1"], "sort_key": None, "dist_key": None},
            {
                "name": "table2",
                "columns": ["col2", "col3"],
                "sort_key": "key2",
                "dist_key": "extra",
            },
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_numeric_values(self):
        """Test with numeric values that should be converted"""
        blended_parameter = "table1:col1:key1:1:10"
        keys = ["name", "columns", "sort_key", "start_id", "end_id"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)

        expected = [
            {
                "name": "table1",
                "columns": ["col1"],
                "sort_key": "key1",
                "start_id": 1,
                "end_id": 10,
            }
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_whitespace(self):
        """Test with whitespace in the input"""
        blended_parameter = " table1 : col1, col2 : key1 ; table2 : * : key2 "
        keys = ["name", "columns", "sort_key"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)

        expected = [
            {"name": "table1", "columns": ["col1", "col2"], "sort_key": "key1"},
            {"name": "table2", "columns": ["*"], "sort_key": "key2"},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_empty_values(self):
        """Test with empty values in the input"""
        blended_parameter = "table1::key1;table2:col1,:"
        keys = ["name", "columns", "sort_key"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)

        expected = [
            {"name": "table1", "columns": None, "sort_key": "key1"},
            {"name": "table2", "columns": ["col1"], "sort_key": None},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_special_characters(self):
        """Test with special characters in values"""
        blended_parameter = "table-1:col.1,col@2:key_1"
        keys = ["name", "columns", "sort_key"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)

        expected = [
            {"name": "table-1", "columns": ["col.1", "col@2"], "sort_key": "key_1"}
        ]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
