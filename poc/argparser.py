from shlex import shlex

def f_p(param_list: list):

    first_level_operators = [">", "<", "=", "!=", ">=", "<=", "eq", "!eq", "%"]
    second_level_operators = ["and", "or"]

    param_store = []
    param_extra = ""
    operation = ()

    for index, param in enumerate(param_list):

        if param in first_level_operators:

            if len(operation) > 0:
                raise Exception("Operator '{}' can't be applied after '{} {}'".format(param, *operation))

            operation += (param_extra+param, param_store if len(param_store) > 1 else param_store[0])
            param_extra = ""
            param_store = []

        elif param in second_level_operators:

            if len(param_store) == 0:
                raise Exception("Missing second comparison argument on {} {}".format(*operation))

            # Operation is finished here
            operation += (param_store if len(param_store) > 1 else param_store[0],)

            # Take care: now there should be another operation at the right of this sentence.
            operation2 = f_p(param_list[index+1:])

            # But, the real operation is far bigger now.
            operation = (param, operation)
            param_store = operation2
            break
        else:
            if param == "!":
                param_extra = "!"
            else:
                param_store.append(param)


    if len(operation) == 2 and len(param_store) == 0:
        raise Exception("Missing second comparison argument on {} {}".format(*operation))

    operation += (param_store if len(param_store) > 1 else param_store[0],)

    return operation


def find_params(text: str):

    sentences = list(shlex(text))

    return f_p(sentences)

def to_mongo_query(operations: tuple):

    result = {}

    if len(operations) > 0:
        operation = operations[0]
        operands = list(operations[1:])

        try:
            if type(operands[0]) is list:
                operands[0] = "".join(operands[0])
            if operands[0].startswith("'"):
                operands[0] = operands[0][1:-1]
            if operands[1].startswith("'"):
                operands[1] = operands[1][1:-1]
        except:
            pass

        if operation == "and":
            result["$and"] = [to_mongo_query(operands[0]), to_mongo_query(operands[1])]
        elif operation == "eq":
            result[operands[0]] = operands[1]
        elif operation == "!eq":
            result[operands[0]] = {"$ne": operands[1]}
        elif operation == "=":
            result[operands[0]] = float(operands[1])
        elif operation == "!=":
            result[operands[0]] = {"$ne": float(operands[1])}
        elif operation == ">":
            result[operands[0]] = {"$gt": float(operands[1])}
        elif operation == "<":
            result[operands[0]] = {"$lt": float(operands[1])}
        elif operation == "<=":
            result[operands[0]] = {"$lt": float(operands[1])+1}
        elif operation == ">=":
            result[operands[0]] = {"$lt": float(operands[1])-1}
        elif operation == "or":
            result["$or"] = [to_mongo_query(operands[0]), to_mongo_query(operands[1])]
        elif operation == "%":
            result[operands[0]] = {"$regex": operands[1]}

    return result

"""
operations = find_params("value.hi != 5 or (value.hi > 2 and value.hi < 4) or key % 'example4'")

{
    "$or": [
        {"value.hi": { "$neq": 5 }},
        {"$and": [
            {"value.hi": {"$gt": 2}},
            {"value.hi": {"$lt": 4}}
        ]},
        {"key": {"$regex": "example4"}}
    ]
}

print(operations)
print(to_mongo_query(operations))
"""

def safe_split(text:str):
    split_chars_order = {
        "'": 2,
        "(": 1,
        ")": 1,
        " ": 0
    }

    split_order_level = 0
    split_segment_open = False
    detected_split_order_level = 0

    segments = []
    segment = ""
    for l in text:

        try:
            detected_split_order_level = split_chars_order[l]
        except KeyError:
            detected_split_order_level = -1

        if detected_split_order_level > split_order_level:
            split_order_level = detected_split_order_level
            split_segment_open = True

        elif detected_split_order_level == split_order_level:

            if split_order_level == 0 or split_segment_open:
                segments.append(segment)
                segment = ""
                split_segment_open = False
                split_order_level = 0
            else:
                segment += l
        else:
            segment += l

    if len(segment) > 0:
        segments.append(segment)

    return segments


def do_split(text:str, separator:str=" ", open_splits:list=None, close_splits:list=None):

    if open_splits is None:
        open_splits = []

    if close_splits is None:
        close_splits = []

    segments = []
    segment = ""

    l_1 = ""
    l_2 = ""
    index = 0

    while index < len(text):
        l_1 = l_2
        l_2 = text[index]

        if l_2 in open_splits and l_1 == separator:
            open_split_index = open_splits.index(l_2)

            segment = do_encapsulated_split(text[index:], sep_init=l_2, sep_end=close_splits[open_split_index])
            segments.append(segment)
            index += len(segment) + 2
            segment = ""
        elif l_2 == separator:
            segments.append(segment)
            segment = ""
        else:
            segment += l_2

        index += 1

    if len(segment) > 0:
        segments.append(segment)

    return segments


def do_encapsulated_split(text:str, sep_init="(", sep_end=")"):

    segment = ""
    open = False

    for index, l in enumerate(text):

        if l == sep_init and not open:
            open = True

        elif l == sep_end:
            open = False

        elif open:
            segment += l

    return segment


def retrieve_ops2_tree(splits:list):
    first_level_operators = [">", "<", "=", "!=", ">=", "<=", "eq", "!eq", "%"]

    # splits should be a 3 elements list : operand1 operator operand2
    if len(splits) < 3:
        raise Exception("Invalid syntax on {}".format(" ".join(splits)))

    operator = splits[1]
    if operator not in first_level_operators:
        raise Exception("Invalid operator on {}".format(" ".join(splits)))

    previous_oper = splits[0:1]
    next_oper = splits[2:]
    return [operator, previous_oper, next_oper]


def retrieve_ops_tree(splits:list, open_splits:list=None, close_splits:list=None):
    second_level_operators = ["and", "or"]

    operators = {operator: [] for operator in second_level_operators+ [None]}

    previous_index = 0
    last_operand = None

    for index, operand in enumerate(splits):
        if operand in operators:
            last_operand = operand
            previous_oper = splits[previous_index:index]

            if len(previous_oper) > 1:
                previous_oper = retrieve_ops2_tree(previous_oper)
            else:
                previous_oper = retrieve_ops_tree(do_split(previous_oper[0], open_splits=open_splits, close_splits=close_splits))

            operators[operand].append(previous_oper)
            previous_index = index + 1

    if previous_index < index + 1:
        previous_oper = splits[previous_index:]

        if len(previous_oper) > 1:
            previous_oper = retrieve_ops2_tree(previous_oper)
        else:
            previous_oper = retrieve_ops_tree(previous_oper)

        operators[last_operand].append(previous_oper)

    return operators

def to_mongo_query_list(ops_list:list):

    [operation, operand1, operand2] = ops_list
    operand1 = operand1[0]
    operand2 = operand2[0]

    result = {}

    if operation == "eq":
        result[operand1] = operand2
    elif operation == "!eq":
        result[operand1] = {"$ne": operand2}
    elif operation == "=":
        result[operand1] = float(operand2)
    elif operation == "!=":
        result[operand1] = {"$ne": float(operand2)}
    elif operation == ">":
        result[operand1] = {"$gt": float(operand2)}
    elif operation == "<":
        result[operand1] = {"$lt": float(operand2)}
    elif operation == "<=":
        result[operand1] = {"$lt": float(operand2)+1}
    elif operation == ">=":
        result[operand1] = {"$lt": float(operand2)-1}
    elif operation == "%":
        result[operand1] = {"$regex": operand2}

    return result

def to_mongo_query_dict(ops_dict):

    ors = ops_dict['or']
    ands = ops_dict['and']
    base = ops_dict[None]

    result = {}
    if len(ors) > 0:
        list_ors = []

        for op in ors:
            if type(op) is list:
                l = to_mongo_query_list(op)
            elif type(op) is dict:
                l = to_mongo_query_dict(op)

            list_ors += [l]

        result['$or'] = list_ors

    if len(ands) > 0:
        list_ands = []
        for op in ands:
            if type(op) is list:
                l = to_mongo_query_list(op)
            elif type(op) is dict:
                l = to_mongo_query_dict(op)
            list_ands += [l]

        if len(result) > 0:
            result['$or'] += list_ands
        else:
            result['$and'] = list_ands

    if len(base) > 0:
        result = to_mongo_query_list(base)

    return result


"""
splits = do_split("value.hi != 5 or (value.hi > 2 and value.hi < 4) or key % 'example4'", open_splits=["(", "'"], close_splits=[")", "'"])
print(retrieve_ops_tree(splits))

splits = do_split("value.hi != 5", open_splits=["(", "'"], close_splits=[")", "'"])
print(retrieve_ops_tree(splits))
"""
splits = do_split("value.hi != 5 or (value.hi > 2 and value.hi < 4) or key % 'example4'", open_splits=["(", "'"], close_splits=[")", "'"])
ops = retrieve_ops_tree(splits, open_splits=["(", "'"], close_splits=[")", "'"])
print(ops)
mongo = to_mongo_query_dict(ops)
print(mongo)

#print(retrieve_ops2_tree(do_split("value.hi = 5")))
#print(do_encapsulated_split("hello (me) llamo ivan"))

"""


def split(text:str):
    split_chars_order = {
        "'": 2,
        "(": 1,
        ")": 1,
        " ": 0
    }

    segment = ""
    segments = []
    current_split_char_level =
    for l in text:
        try:
            split_char_level = split_chars_order[l]

        except KeyError:
            split_char_level = -1

        if current_split_char_level == 0:
            segments.append(segment)
            segment = ""
        else:
            segment += l

    return segments

print(split("hello '(me llamo)'ivan"))

"""