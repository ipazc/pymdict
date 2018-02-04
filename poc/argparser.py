from shlex import shlex

def f_p(param_list: list):

    first_level_operators = [">", "<", "=", ">=", "<="]
    second_level_operators = ["and", "or"]

    param_store = []
    operation = ()

    for index, param in enumerate(param_list):

        if param in first_level_operators:

            if len(operation) > 0:
                raise Exception("Operator '{}' can't be applied after '{} {}'".format(param, *operation))

            operation += (param, param_store if len(param_store) > 1 else param_store[0])
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
            if operands[0].startswith("'"):
                operands[0] = operands[0][1:-1]
            if operands[1].startswith("'"):
                operands[1] = operands[1][1:-1]
        except:
            pass

        if operation == "and":
            result["$and"] = [to_mongo_query(operands[0]), to_mongo_query(operands[1])]
        elif operation == "=":
            result[operands[0]] = operands[1]
        elif operation == ">":
            result["$gt"] = {operands[0]: operands[1]}
        elif operation == "<":
            result["$lt"] = {operands[0]: operands[1]}
        elif operation == "<=":
            result["$lt"] = {operands[0]: operands[1]+1}
        elif operation == ">=":
            result["$lt"] = {operands[0]: operands[1]-1}
        elif operation == "or":
            result["$or"] = [to_mongo_query(operands[0]), to_mongo_query(operands[1])]

    return result


operations = find_params("A = '2 + 4' and B = 2 or C = '34'")

print(operations)
print(to_mongo_query(operations))