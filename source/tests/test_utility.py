import unittest
from typing import List
from utility import prepare_dictionaries_from_blended_parameter


class TestUtility(unittest.TestCase):
    def test_prepare_dictionaries_basic(self):
        """Test basic functionality with table configuration"""
        blended_parameter = "table1:col1,col2:key1;table2:*:key2"
        keys = ["name", "columns", "sort_key"]
        types = [str, list, str]
        result = prepare_dictionaries_from_blended_parameter(
            blended_parameter, keys, types
        )

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
        types = [str, list, str, str]
        result = prepare_dictionaries_from_blended_parameter(
            blended_parameter, keys, types
        )

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

    def test_prepare_dictionaries_with_types(self):
        """Test with type conversion"""
        blended_parameter = "table1:10:key1;table2:20:key2"
        keys = ["name", "id", "sort_key"]
        types = [str, int, str]
        result = prepare_dictionaries_from_blended_parameter(
            blended_parameter, keys, types
        )

        expected = [
            {"name": "table1", "id": 10, "sort_key": "key1"},
            {"name": "table2", "id": 20, "sort_key": "key2"},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_with_list_type(self):
        """Test with list type conversion"""
        blended_parameter = "table1:col1,col2:key1;table2:col3,col4:key2"
        keys = ["name", "cols", "keys"]
        types = [str, list, str]
        result = prepare_dictionaries_from_blended_parameter(
            blended_parameter, keys, types
        )

        expected = [
            {"name": "table1", "cols": ["col1", "col2"], "keys": "key1"},
            {"name": "table2", "cols": ["col3", "col4"], "keys": "key2"},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_type_conversion_error(self):
        """Test handling of type conversion errors"""
        blended_parameter = "table1:not_a_number:key1"
        keys = ["name", "id", "sort_key"]
        types = [str, int, str]

        with self.assertRaises(ValueError) as context:
            prepare_dictionaries_from_blended_parameter(blended_parameter, keys, types)

        self.assertTrue("Failed to convert value" in str(context.exception))

    def test_prepare_dictionaries_invalid_types_length(self):
        """Test validation of types list length"""
        blended_parameter = "table1:col1:key1"
        keys = ["name", "columns", "sort_key"]
        types = [str, list]  # Missing one type

        with self.assertRaises(ValueError) as context:
            prepare_dictionaries_from_blended_parameter(blended_parameter, keys, types)

        self.assertEqual(
            str(context.exception), "Length of types must match length of keys"
        )

    def test_prepare_dictionaries_absent_types_parameter(self):
        """Test all values should be treated as string when types parameter is absent"""
        blended_parameter = "table1:10:key1;table2:20:key2"
        keys = ["name", "id", "sort_key"]
        result = prepare_dictionaries_from_blended_parameter(blended_parameter, keys)

        # Without types parameter, all values should remain as strings
        expected = [
            {"name": "table1", "id": "10", "sort_key": "key1"},
            {"name": "table2", "id": "20", "sort_key": "key2"},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_whitespace(self):
        """Test with whitespace in the input"""
        blended_parameter = " table1 : col1, col2 : key1 ; table2 : * : key2 "
        keys = ["name", "columns", "sort_key"]
        types = [str, list, str]
        result = prepare_dictionaries_from_blended_parameter(
            blended_parameter, keys, types
        )

        expected = [
            {"name": "table1", "columns": ["col1", "col2"], "sort_key": "key1"},
            {"name": "table2", "columns": ["*"], "sort_key": "key2"},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_empty_values(self):
        """Test with empty values in the input"""
        blended_parameter = "table1::key1;table2:col1,:"
        keys = ["name", "columns", "sort_key"]
        types = [str, list, str]
        result = prepare_dictionaries_from_blended_parameter(
            blended_parameter, keys, types
        )

        expected = [
            {"name": "table1", "columns": None, "sort_key": "key1"},
            {"name": "table2", "columns": ["col1"], "sort_key": None},
        ]
        self.assertEqual(result, expected)

    def test_prepare_dictionaries_special_characters(self):
        """Test with special characters in values"""
        blended_parameter = "table-1:col.1,col@2:key_1"
        keys = ["name", "columns", "sort_key"]
        types = [str, list, str]
        result = prepare_dictionaries_from_blended_parameter(
            blended_parameter, keys, types
        )

        expected = [
            {"name": "table-1", "columns": ["col.1", "col@2"], "sort_key": "key_1"}
        ]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
