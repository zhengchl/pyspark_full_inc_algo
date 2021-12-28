import itertools
from functools import partial

from pyspark.sql import functions as funs
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, LongType, StringType, DoubleType, FloatType, BooleanType

from full_inc_algo import type_generic


class FullIncAlgo:
    __slots__ = ['_inc_df', '_full_df', '_primary_key_col_names',
                 '_second_key_col_names', '_has_second_key', '_value_col_names',
                 '_value_type_map', '_full_history_times']

    def __init__(self,
                 inc_df: DataFrame,
                 primary_key_col_names: list, second_key_col_names: list, value_col_names: list,
                 full_df: DataFrame = None,
                 full_history_times: int = 30):
        self._full_history_times = full_history_times
        self._inc_df = inc_df
        self._full_df = full_df
        self._primary_key_col_names = primary_key_col_names
        self._second_key_col_names = second_key_col_names
        self._has_second_key = True if len(second_key_col_names) > 0 else False
        self._value_col_names = value_col_names
        self._check_schema()
        self._check_col_names()
        self._value_type_map = self._get_value_type()

    def _check_col_names(self):
        if len(set(self._primary_key_col_names)) != len(self._primary_key_col_names):
            raise ValueError(
                f'Args primary_key_col_names has duplicate name {self._primary_key_col_names}')
        if len(set(self._second_key_col_names)) != len(self._second_key_col_names):
            raise ValueError(
                f'Args second_key_col_names has duplicate name {self._second_key_col_names}')
        if len(set(self._value_col_names)) != len(self._value_col_names):
            raise ValueError(
                f'Args value_col_names has duplicate name {self._value_col_names}')

        if len(set(self._primary_key_col_names).intersection(self._second_key_col_names)):
            raise ValueError('Args primary_key_col_names and second_key_col_names have duplicate name' +
                             f'{self._primary_key_col_names} {self._second_key_col_names}')
        if len(set(self._value_col_names).intersection(self._second_key_col_names)):
            raise ValueError('Args value_col_names and second_key_col_names have duplicate name' +
                             f'{self._value_col_names} {self._second_key_col_names}')
        if len(set(self._primary_key_col_names).intersection(self._value_col_names)):
            raise ValueError('Args primary_key_col_names and value_col_names have duplicate name' +
                             f'{self._primary_key_col_names} {self._value_col_names}')

    def _check_schema(self):
        """检查增量表的schema"""
        schema = self._inc_df.schema
        field_names = schema.fieldNames()

        for name in itertools.chain(self._primary_key_col_names,
                                    self._second_key_col_names):
            if name not in field_names:
                raise NameError(f"Field name: {name} not in inc_df schema")
            field_type = schema[name].dataType
            if not (isinstance(field_type, StringType) or
                    isinstance(field_type, IntegerType) or isinstance(field_type, LongType)):
                raise TypeError(
                    f"Field {name} of type {field_type}, cannot be key")

        for name in self._value_col_names:
            if name not in field_names:
                raise NameError(f"Field name: {name} not in inc_df schema")
            field_type = schema[name].dataType
            if not (isinstance(field_type, StringType) or isinstance(field_type, IntegerType) or
                    isinstance(field_type, BooleanType) or isinstance(field_type, LongType) or
                    isinstance(field_type, FloatType) or isinstance(field_type, DoubleType)):
                raise TypeError(
                    f"Field {name} of type {field_type}, cannot be key")

    def _get_value_type(self) -> dict:
        value_type_map = {}
        schema = self._inc_df.schema
        for name in self._value_col_names:
            field_type = schema[name].dataType
            if isinstance(field_type, StringType):
                value_type_map[name] = 'str'
            elif isinstance(field_type, IntegerType) or isinstance(field_type, LongType):
                value_type_map[name] = 'int'
            elif isinstance(field_type, FloatType) or isinstance(field_type, DoubleType):
                value_type_map[name] = 'float'
            elif isinstance(field_type, BooleanType):
                value_type_map[name] = 'bool'
        return value_type_map

    def print_stat_table_schema(self):
        """打印统计表的schema，以便创建hive表"""
        schema = self._inc_df.schema
        rtn_schema = []
        hive_type_map = {'StringType': 'string',
                         'IntegerType': 'int',
                         'LongType': 'bigint',
                         'FloatType': 'float',
                         'DoubleType': 'double',
                         'BooleanType': 'boolean'}
        for name in self._primary_key_col_names:
            field_type = schema[name].dataType
            rtn_schema.append(f"`{name}` {hive_type_map[str(field_type)]}")

        for name in self._value_col_names:
            field_type = schema[name].dataType
            if self._has_second_key:
                rtn_schema.append(
                    f"`{name}_full_data` Map< string, Array< {hive_type_map[str(field_type)]} > >")
            else:
                rtn_schema.append(
                    f"`{name}_full_data` Array< {hive_type_map[str(field_type)]} >")

        return ',\n'.join(rtn_schema)

    @staticmethod
    def _process_none(full, inc, has_second_key):
        assert (not (full is None and inc is None))
        rtn_full = full
        rtn_inc = inc

        if rtn_full is None:
            if has_second_key:
                rtn_full = {}
            else:
                rtn_full = []

        if rtn_inc is None:
            if has_second_key:
                infer_value = next(iter(rtn_full))
                rtn_inc = list(type_generic.get_zero(infer_value))
            else:
                infer_value = rtn_full[0]
                rtn_inc = type_generic.get_zero(infer_value)

        return rtn_full, rtn_inc

    @staticmethod
    def _update_row_with_second_key(full: dict, inc_list: list, type_str: str,
                                    has_second_key: bool, full_history_times: int):
        full, inc_list = FullIncAlgo._process_none(full, inc_list, has_second_key)

        assert (isinstance(full, dict))
        for inc in inc_list:
            loc = inc.rfind(':')
            second_key = inc[: loc]
            value = type_generic.str_2_value(inc[loc + 1:], type_str)
            if second_key in full:
                full[second_key].append(value)
            else:
                full[second_key] = [value]

        for second_key, value_list in full:
            if len(value_list) > full_history_times:
                full[second_key] = value_list[1:]

        for second_key, value_list in full:
            if type_generic.is_empty_info(value_list):
                del full[second_key]

        if len(full) == 0:
            return None

        return full

    @staticmethod
    def _update_row_normal(full: list, inc,
                           has_second_key: bool, full_history_times: int):
        full, inc = FullIncAlgo._process_none(full, inc, has_second_key)

        assert (isinstance(full, list))
        full.append(inc)
        if len(full) > full_history_times:
            full = full[1:]

        if type_generic.is_empty_info(full):
            return None

        return full

    def run_normal(self):
        update_row = partial(FullIncAlgo._update_row_normal,
                             has_second_key=self._has_second_key, full_history_times=self._full_history_times)
        update_row_udf = funs.udf(update_row).asNondeterministic()
        if self._full_df is None:
            join_df = self._inc_df
            for name in self._value_col_names:
                join_df = join_df.withColumn(
                    f'{name}_full_data', funs.lit(None))
        else:
            join_df = self._full_df.join(
                self._inc_df, on=self._primary_key_col_names, how='full')

        rtn_df = join_df
        for name in self._value_col_names:
            rtn_df = (rtn_df
                      .withColumn(f'new_{name}_full_data',
                                  update_row_udf(funs.col(f'{name}_full_data'), funs.col(name)))
                      .drop(f'{name}_full_data')
                      .withColumnRenamed(f'new_{name}_full_data', f'{name}_full_data')
                      .cache()
                      )
        return rtn_df

    def run_with_second_key(self):
        agg_args = []
        for name in self._value_col_names:
            concat_args = []
            for second_key in self._second_key_col_names:
                concat_args.append(second_key)
                concat_args.append(',')
            concat_args[-1] = ':'
            concat_args.append(name)
            agg_args.append(funs.collect_list(
                funs.concat(*concat_args)).alias(f'{name}_list'))
        group_df = self._inc_df.groupBy(
            self._primary_key_col_names).agg(*agg_args)

        if self._full_df is None:
            join_df = group_df
            for name in self._value_col_names:
                join_df = join_df.withColumn(
                    f'{name}_full_data', funs.lit(None))
        else:
            join_df = self._full_df.join(
                group_df, on=self._primary_key_col_names, how='full')

        update_row = partial(FullIncAlgo._update_row_with_second_key,
                             has_second_key=self._has_second_key, full_history_times=self._full_history_times)
        update_row_udf = funs.udf(update_row).asNondeterministic()
        rtn_df = join_df
        for name in self._value_col_names:
            value_type = self._value_type_map[name]
            rtn_df = (rtn_df
                      .withColumn(f'new_{name}_full_data', update_row_udf(
                funs.col(f'{name}_full_data'), funs.col(name), funs.lit(value_type)))
                      .drop(f'{name}_full_data')
                      .withColumnRenamed(f'new_{name}_full_data', f'{name}_full_data')
                      .cache()
                      )
        return rtn_df

    def run(self):
        if self._has_second_key:
            return self.run_with_second_key()

        return self.run_normal()