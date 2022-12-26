def tuple_map(func, x):
    return tuple(map(func, x))


def load_data(path="../../data/msnbc/data.seq"):
    lines = open(path, "r").readlines()
    lines = map(str.split, lines)

    def to_int_list(x):
        return int(x),

    def line_to_int_list(line):
        return tuple_map(to_int_list, line)

    lines = tuple_map(line_to_int_list, lines)
    return lines
