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
    if type_str == 'int':
        return int(value)

    if type_str == 'float':
        return float(value)

    if type_str == 'str':
        return value

    if type_str == 'bool':
        return bool(value)
    
    return None
