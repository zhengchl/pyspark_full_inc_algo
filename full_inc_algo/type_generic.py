from pyspark.sql.types import IntegerType, LongType, StringType, DoubleType, FloatType, BooleanType

spark_type_name_map = {
    'IntegerType': IntegerType().typeName(),
    'LongType': LongType().typeName(),
    'StringType': StringType().typeName(),
    'DoubleType': DoubleType().typeName(),
    'FloatType': FloatType().typeName(),
    'BooleanType': BooleanType().typeName()
}


def is_empty_info(info_list):
    infer_value = info_list[0]
    if isinstance(infer_value, int) or isinstance(infer_value, float):
        return sum(info_list) == 0

    if isinstance(infer_value, str):
        return all(s == '' for s in info_list)

    if isinstance(infer_value, bool):
        return all(s is False for s in info_list)

    return False


def get_zero(infer_value):
    if isinstance(infer_value, int):
        return 0

    if isinstance(infer_value, float):
        return 0.0

    if isinstance(infer_value, str):
        return ''

    if isinstance(infer_value, bool):
        return False

    raise TypeError(f'Unsupported type {type(infer_value)}')


def str_2_value(value, type_str):
    if type_str == spark_type_name_map['IntegerType'] or type_str == spark_type_name_map['LongType']:
        return int(value)

    if type_str == spark_type_name_map['FloatType'] or type_str == spark_type_name_map['DoubleType']:
        return float(value)

    if type_str == spark_type_name_map['StringType']:
        return value

    if type_str == spark_type_name_map['BooleanType']:
        return bool(value)

    return None


def str_2_spark_type(type_str):
    if type_str == spark_type_name_map['IntegerType']:
        return IntegerType()

    if type_str == spark_type_name_map['LongType']:
        return LongType()

    if type_str == spark_type_name_map['FloatType']:
        return FloatType()

    if type_str == spark_type_name_map['DoubleType']:
        return DoubleType()

    if type_str == spark_type_name_map['StringType']:
        return StringType()

    if type_str == spark_type_name_map['BooleanType']:
        return BooleanType()

    return None
