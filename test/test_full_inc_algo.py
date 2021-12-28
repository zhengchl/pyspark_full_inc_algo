import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, FloatType, StructType, StringType, IntegerType, \
    StructField, LongType, MapType

from pyspark_test import assert_pyspark_df_equal

from full_inc_algo import FullIncAlgo


class TestFullIncAlgo(unittest.TestCase):
    def setUp(self) -> None:
        day1 = [['user1', 101, 'dev1_1', 10101, 'day1_u1_h1'],
                ['user1', 102, 'dev1_2', 10102, 'day1_u1_h2'],
                ['user2', 201, 'dev2_1', 10201, 'day1_u2_h1'],
                ['user2', 202, 'dev2_2', 10202, 'day1_u2_h2'],
                ['user3', 301, 'dev3_1', 10301, 'day1_u3_h1']]
        day2 = [['user1', 101, 'dev1_1', 10101, 'day2_u1_h1'],
                ['user1', 102, 'dev1_2', 10102, 'day2_u1_h2'],
                ['user3', 301, 'dev3_1', 10301, 'day2_u3_h1'],
                ['user3', 302, 'dev3_2', None, 'day2_u3_h2'],
                ['user4', 401, 'dev4_1', 10401, 'day2_u4_h1'],
                ['user4', 402, 'dev4_2', 10402, 'day2_u4_h2']]
        day3 = [['user2', 201, 'dev2_1', 10201, 'day1_u2_h1']]

        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.sc = self.spark.sparkContext

        schema = StructType([
            StructField('uid', StringType(), True),
            StructField('rid', IntegerType(), True),
            StructField('did', StringType(), True),
            StructField('num', LongType(), True),
            StructField('desc', StringType(), True)
        ])
        self.day1_df = self.sc.parallelize(day1).toDF(schema)
        self.day2_df = self.sc.parallelize(day2).toDF(schema)
        self.day3_df = self.sc.parallelize(day3).toDF(schema)

        type_schema = StructType([
            StructField('str_field', StringType(), True),
            StructField('list_field', ArrayType(IntegerType(), True), True),
            StructField('map_field', MapType(
                StringType(), StringType(), True), True),
            StructField('int_field', IntegerType(), True),
            StructField('bigint_field', LongType(), True),
            StructField('float_field', FloatType(), True),
            StructField('double_field', DoubleType(), True),
            StructField('bool_field', BooleanType(), True)]
        )
        self.type_df = self.sc.emptyRDD().toDF(type_schema)

    def test_print_stat_table_schema(self):
        schema1 = "`uid` string,\n" + \
                  "`num_history_` Array< bigint >,\n" + \
                  "`desc_history_` Array< string >"
        algo1 = FullIncAlgo(self.day1_df, ['uid'], [], ['num', 'desc'], None)
        self.assertEqual(algo1.print_stat_table_schema(), schema1)

        schema2 = "`uid` string,\n" + \
                  "`rid` int,\n" + \
                  "`num_history_` Map< string, Array< bigint > >,\n" + \
                  "`desc_history_` Map< string, Array< string > >"
        algo2 = FullIncAlgo(self.day1_df, ['uid', 'rid'], [
            'did'], ['num', 'desc'], None)
        self.assertEqual(algo2.print_stat_table_schema(), schema2)

        schema3 = "`rid` int,\n" + \
                  "`num_history_` Map< string, Array< bigint > >,\n" + \
                  "`desc_history_` Map< string, Array< string > >"
        algo3 = FullIncAlgo(self.day1_df, ['rid'], [
            'uid', 'did'], ['num', 'desc'], None)
        self.assertEqual(algo3.print_stat_table_schema(), schema3)

    def test_check_col_names(self):
        self.assertRaises(ValueError, FullIncAlgo,
                          inc_df=self.day1_df, primary_key_col_names=[
                              'rid', 'rid'],
                          second_key_col_names=['uid', 'did'], value_col_names=['num', 'desc'])
        self.assertRaises(ValueError, FullIncAlgo,
                          inc_df=self.day1_df, primary_key_col_names=[
                              'rid', 'uid'],
                          second_key_col_names=['uid', 'did'], value_col_names=['num', 'desc'])
        self.assertRaises(ValueError, FullIncAlgo,
                          inc_df=self.day1_df, primary_key_col_names=['rid'],
                          second_key_col_names=['uid', 'did'], value_col_names=['did', 'num', 'desc'])

    def test_check_schema(self):
        self.assertRaises(TypeError, FullIncAlgo,
                          inc_df=self.type_df, primary_key_col_names=[
                              'map_field'],
                          second_key_col_names=[], value_col_names=[])
        self.assertRaises(TypeError, FullIncAlgo,
                          inc_df=self.type_df, primary_key_col_names=[
                              'int_field'],
                          second_key_col_names=['float_field'], value_col_names=[])
        FullIncAlgo(inc_df=self.type_df, primary_key_col_names=[
            'int_field'],
            second_key_col_names=['bigint_field'], value_col_names=['float_field'])

    def test_run_normal(self):
        full_day1 = [['dev1_1', [10101], ['day1_u1_h1']],
                     ['dev1_2', [10102], ['day1_u1_h2']],
                     ['dev2_1', [10201], ['day1_u2_h1']],
                     ['dev2_2', [10202], ['day1_u2_h2']],
                     ['dev3_1', [10301], ['day1_u3_h1']]]
        full_day1_day2 = [['dev1_1', [10101, 10101], ['day1_u1_h1', 'day2_u1_h1']],
                          ['dev1_2', [10102, 10102], ['day1_u1_h2', 'day2_u1_h2']],
                          ['dev2_1', [10201, None], ['day1_u2_h1', None]],
                          ['dev2_2', [10202, None], ['day1_u2_h2', None]],
                          ['dev3_1', [10301, 10301], ['day1_u3_h1', 'day2_u3_h1']],
                          ['dev3_2', None, ['day2_u3_h2']],
                          ['dev4_1', [10401], ['day2_u4_h1']],
                          ['dev4_2', [10402], ['day2_u4_h2']]]
        full_day1_day2_day3 = [['dev1_1', [10101, 10101, None], ['day1_u1_h1', 'day2_u1_h1', None]],
                               ['dev1_2', [10102, 10102, None], [
                                   'day1_u1_h2', 'day2_u1_h2', None]],
                               ['dev2_1', [10201, None, 10201], [
                                   'day1_u2_h1', None, 'day1_u2_h1']],
                               ['dev2_2', [10202, None, None],
                                   ['day1_u2_h2', None, None]],
                               ['dev3_1', [10301, 10301, None], [
                                   'day1_u3_h1', 'day2_u3_h1', None]],
                               ['dev3_2', None, ['day2_u3_h2', None]],
                               ['dev4_1', [10401, None], ['day2_u4_h1', None]],
                               ['dev4_2', [10402, None], ['day2_u4_h2', None]]]
        full_day2_day3 = [['dev1_1', [10101, None], ['day2_u1_h1', None]],
                          ['dev1_2', [10102, None], ['day2_u1_h2', None]],
                          ['dev2_1', [None, 10201], [None, 'day1_u2_h1']],
                          ['dev3_1', [10301, None], ['day2_u3_h1', None]],
                          ['dev3_2', None, ['day2_u3_h2', None]],
                          ['dev4_1', [10401, None], ['day2_u4_h1', None]],
                          ['dev4_2', [10402, None], ['day2_u4_h2', None]]]

        schema = StructType([
            StructField('did', StringType(), True),
            StructField(f'num_{FullIncAlgo.HISTORY_COL_SUFFIX}',
                        ArrayType(LongType()), True),
            StructField(f'desc_{FullIncAlgo.HISTORY_COL_SUFFIX}',
                        ArrayType(StringType()), True)
        ])
        expected_full_day1_df = self.sc.parallelize(full_day1).toDF(schema)
        expected_full_day1_day2_df = self.sc.parallelize(
            full_day1_day2).toDF(schema)
        expected_full_day1_day2_day3_df = self.sc.parallelize(
            full_day1_day2_day3).toDF(schema)
        expected_full_day2_day3_df = self.sc.parallelize(
            full_day2_day3).toDF(schema)

        algo1 = FullIncAlgo(self.day1_df, ['did'], [], ['num', 'desc'], None)
        real_full_day1 = algo1.run().select('did', 'num_history_', 'desc_history_')
        assert_pyspark_df_equal(
            real_full_day1, expected_full_day1_df, check_column_names=True)

        algo2 = FullIncAlgo(self.day2_df, ['did'], [], [
                            'num', 'desc'], real_full_day1)
        real_full_day1_day2_df = algo2.run().select(
            'did', 'num_history_', 'desc_history_')
        assert_pyspark_df_equal(
            real_full_day1_day2_df, expected_full_day1_day2_df, check_column_names=True, order_by=['did'])

        algo3 = FullIncAlgo(self.day3_df, ['did'], [], [
                            'num', 'desc'], real_full_day1_day2_df)
        real_full_day1_day2_day3_df = algo3.run().select(
            'did', 'num_history_', 'desc_history_')
        assert_pyspark_df_equal(real_full_day1_day2_day3_df,
                                expected_full_day1_day2_day3_df, check_column_names=True, order_by=['did'])

        algo4 = FullIncAlgo(self.day3_df, ['did'], [], [
                            'num', 'desc'], real_full_day1_day2_df, 2)
        real_full_day2_day3_df = algo4.run().select(
            'did', 'num_history_', 'desc_history_')
        assert_pyspark_df_equal(
            real_full_day2_day3_df, expected_full_day2_day3_df, check_column_names=True, order_by=['did'])

    def test_run_with_second_key(self):
        day1 = [['user1', 101, 'dev1_1', 10101, 'day1_u1_h1'],
                ['user1', 102, 'dev1_2', 10102, 'day1_u1_h2'],
                ['user2', 201, 'dev2_1', 10201, 'day1_u2_h1'],
                ['user2', 202, 'dev2_2', 10202, 'day1_u2_h2'],
                ['user3', 301, 'dev3_1', 10301, 'day1_u3_h1']]
        day2 = [['user1', 101, 'dev1_1', 10101, 'day2_u1_h1'],
                ['user1', 102, 'dev1_2', 10102, 'day2_u1_h2'],
                ['user3', 301, 'dev3_1', 10301, 'day2_u3_h1'],
                ['user3', 302, 'dev3_2', None, 'day2_u3_h2'],
                ['user4', 401, 'dev4_1', 10401, 'day2_u4_h1'],
                ['user4', 402, 'dev4_2', 10402, 'day2_u4_h2']]
        day3 = [['user2', 201, 'dev2_1', 10201, 'day1_u2_h1']]

        full_day1 = [['user1', {'101,dev1_1': [10101], '102,dev1_2': [10102]},
                      {'101,dev1_1': ['day1_u1_h1'], '102,dev1_2': ['day1_u1_h2']}],
                     ['user2', {'201,dev2_1': [10201], '202,dev2_2': [10202]},
                      {'201,dev2_1': ['day1_u2_h1'], '202,dev2_2': ['day1_u2_h2']}],
                     ['user3', {'301,dev3_1': [10301]},
                      {'301,dev3_1': ['day1_u3_h1']}],
                     ]
        schema = StructType([
            StructField('uid', StringType(), True),
            StructField(f'num_{FullIncAlgo.HISTORY_COL_SUFFIX}',
                        MapType(StringType(), ArrayType(LongType())), True),
            StructField(f'desc_{FullIncAlgo.HISTORY_COL_SUFFIX}',
                        MapType(StringType(), ArrayType(StringType())), True)
        ])
        expected_full_day1_df = self.sc.parallelize(full_day1).toDF(schema)

        algo1 = FullIncAlgo(self.day1_df, ['uid'], ['rid', 'did'], ['num', 'desc'], None)
        real_full_day1 = algo1.run().select('uid', 'num_history_', 'desc_history_')
        assert_pyspark_df_equal(
            real_full_day1, expected_full_day1_df, check_column_names=True, order_by=['uid'])

if __name__ == '__main__':
    unittest.main()
