from pyspark.sql.types import IntegerType, LongType, StringType, DoubleType, FloatType, BooleanType

spark_type_name_map = {
    'IntegerType': IntegerType().typeName(),
    'LongType': LongType().typeName(),
    'StringType': StringType().typeName(),
    'DoubleType': DoubleType().typeName(),
    'FloatType': FloatType().typeName(),
    'BooleanType': BooleanType().typeName()
}

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
