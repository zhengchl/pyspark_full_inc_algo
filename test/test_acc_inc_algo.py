import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, FloatType, StructType, StringType, IntegerType, StructField, LongType, MapType

from acc_inc_algo import AccIncAlgo


class TestAccIncAlgo(unittest.TestCase):
    def setUp(self) -> None:
        day1 = [['user1', 101, 'dev1_1', 10101, 'day1_u1_h1'],
                ['user1', 102, 'dev1_2', 10102, 'day1_u1_h2'],
                ['user2', 201, 'dev2_1', 10201, 'day1_u2_h1'],
                ['user2', 202, 'dev2_2', 10202, 'day1_u2_h2'],
                ['user3', 301, 'dev3_1', 10301, 'day1_u3_h1']]
        day2 = [['user1', 101, 'dev1_1', 10101, 'day2_u1_h1'],
                ['user1', 102, 'dev1_2', 10102, 'day2_u1_h2'],
                ['user3', 301, 'dev3_1', 10301, 'day2_u3_h1'],
                ['user3', 302, 'dev3_2', 10302, 'day2_u3_h2'],
                ['user4', 401, 'dev4_1', 10401, 'day2_u4_h1'],
                ['user4', 402, 'dev4_2', 10402, 'day2_u4_h2']]
        day3 = []

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
        schema1 = "`uid` string,\n" +\
                  "`num_acc_data` Array< bigint >,\n" +\
                  "`desc_acc_data` Array< string >"
        algo1 = AccIncAlgo(self.day1_df, ['uid'], [], ['num', 'desc'], None)
        self.assertEqual(algo1.print_stat_table_schema(), schema1)

        schema2 = "`uid` string,\n" +\
                  "`rid` int,\n" +\
                  "`num_acc_data` Map< string, Array< bigint > >,\n" +\
                  "`desc_acc_data` Map< string, Array< string > >"
        algo2 = AccIncAlgo(self.day1_df, ['uid', 'rid'], [
                           'did'], ['num', 'desc'], None)
        self.assertEqual(algo2.print_stat_table_schema(), schema2)

        schema3 = "`rid` int,\n" +\
                  "`num_acc_data` Map< string, Array< bigint > >,\n" +\
                  "`desc_acc_data` Map< string, Array< string > >"
        algo3 = AccIncAlgo(self.day1_df, ['rid'], [
                           'uid', 'did'], ['num', 'desc'], None)
        self.assertEqual(algo3.print_stat_table_schema(), schema3)

    def test_check_col_names(self):
        self.assertRaises(ValueError, AccIncAlgo,
                          inc_df=self.day1_df, primary_key_col_names=[
                              'rid', 'rid'],
                          second_key_col_names=['uid', 'did'], value_col_names=['num', 'desc'])
        self.assertRaises(ValueError, AccIncAlgo,
                          inc_df=self.day1_df, primary_key_col_names=[
                              'rid', 'uid'],
                          second_key_col_names=['uid', 'did'], value_col_names=['num', 'desc'])
        self.assertRaises(ValueError, AccIncAlgo,
                          inc_df=self.day1_df, primary_key_col_names=['rid'],
                          second_key_col_names=['uid', 'did'], value_col_names=['did', 'num', 'desc'])

    def test_check_schema(self):
        self.assertRaises(TypeError, AccIncAlgo,
                          inc_df=self.type_df, primary_key_col_names=[
                              'map_field'],
                          second_key_col_names=[], value_col_names=[])
        self.assertRaises(TypeError, AccIncAlgo,
                          inc_df=self.type_df, primary_key_col_names=[
                              'int_field'],
                          second_key_col_names=['float_field'], value_col_names=[])
        AccIncAlgo(inc_df=self.type_df, primary_key_col_names=[
            'int_field'],
                   second_key_col_names=['bigint_field'], value_col_names=['float_field'])

    def test_run_normal(self):
        pass
