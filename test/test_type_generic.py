import unittest

from full_inc_algo import type_generic


class TestTypeGeneric(unittest.TestCase):
    def test_str_2_value(self):
        self.assertEqual(type_generic.str_2_value('113', type_generic.spark_type_name_map['IntegerType']), 113)
        self.assertEqual(type_generic.str_2_value('113.1', type_generic.spark_type_name_map['FloatType']), 113.1)
        self.assertEqual(type_generic.str_2_value('113', type_generic.spark_type_name_map['StringType']), '113')
        self.assertEqual(type_generic.str_2_value('113', type_generic.spark_type_name_map['BooleanType']), True)
        self.assertEqual(type_generic.str_2_value('113', 'ag'), None)


if __name__ == '__main__':
    unittest.main()
