import unittest

from full_inc_algo import type_generic


class TestTypeGeneric(unittest.TestCase):
    def test_is_empty_info(self):
        self.assertTrue(type_generic.is_empty_info([0] * 10))
        self.assertTrue(type_generic.is_empty_info([''] * 10))
        self.assertTrue(type_generic.is_empty_info([0.0] * 10))
        self.assertTrue(type_generic.is_empty_info([False] * 10))

        self.assertFalse(type_generic.is_empty_info([1, 0] * 10))
        self.assertFalse(type_generic.is_empty_info([0.0, 1.0] * 10))
        self.assertFalse(type_generic.is_empty_info(['1', ''] * 10))
        self.assertFalse(type_generic.is_empty_info([False, True] * 10))

    def test_get_zero(self):
        self.assertEqual(type_generic.get_zero(111), 0)
        self.assertEqual(type_generic.get_zero(1.11), 0.0)
        self.assertEqual(type_generic.get_zero('111'), '')
        self.assertEqual(type_generic.get_zero(True), False)

        self.assertRaises(TypeError, type_generic.get_zero, args=[1, 's'])

    def test_str_2_value(self):
        self.assertEqual(type_generic.str_2_value('113', 'IntegerType'), 113)
        self.assertEqual(type_generic.str_2_value('113.1', 'FloatType'), 113.1)
        self.assertEqual(type_generic.str_2_value('113', 'StringType'), '113')
        self.assertEqual(type_generic.str_2_value('113', 'BooleanType'), True)
        self.assertEqual(type_generic.str_2_value('113', 'ag'), None)


if __name__ == '__main__':
    unittest.main()
