#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# MIT License
#
# Copyright (c) 2018 Iván de Paz Centeno
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re


class MongoQueryParser():
    """
    Allows to process a string query and to convert it into a mongo query.
    """

    def __init__(self):
        pass


    def transform_request(self, str_query:str):
        """
        Transforms the given request in a mongo query

        :param str_query: String containing the query.

        A request is a set of conditions that elements must satisfy. For example, if we have the following dictionary:

        {
            "father": { "name": "foo", "age": 44},
            "mother": { "name": "bar", "age": 41},
            "sister": { "name": "foobar", "age": 19}
        }

        and it is wanted to get the element with name "foo", it can be done as follows:

            value.name eq "foo"

        This query will return the "father" element.

        Queries can be combined in such a way to make complex queries. Example:

            value.name eq "foo" or (value.age > 40 and value.age < 50)

        This query returns the "father" and "mother" elements.

        Available operators are:

            eq               equals for comparing text
            =                equals for comparing numbers
            !                negation, can be prepended to eq and =, for example: !eq and !=
            >                greater than a number
            <                lesser than a number
            >=               greater or equal than a number
            <=               lesser or equal than a number
            %                Regex match
            !%               Regex not match
            in               in a list of values

            or               To join two conditions in OR
            and              To join two conditions in AND

        :return: JSON query for a MongoDB
        """
        if str_query == "":
            return {}

        splits = self._do_split(str_query, open_splits=["(", "[", "'"], close_splits=[")", "]", "'"],
                                special_separators="[")
        ops = self._retrieve_ops_tree(splits, open_splits=["(", "[", "'"], close_splits=[")", "[", "'"])
        mongo = self._to_mongo_query_dict(ops)

        return mongo

    @staticmethod
    def _do_split(text:str, separator:str=" ", open_splits:list=None, close_splits:list=None, special_separators:list=None):
        """
        Splits a text into chunks. Same functionality as string.split() but it allows to split text in a more complex
        way.


        :param text: text to split
        :param separator: separator to use. For example, a " " will split the text in chunks separated by spaces.
        :param open_splits:  If a more complex separation is required, for example those between brackets, then here
        it can be specified the open token
        :param close_splits:
        :param special_separators:
        :return:
        """

        if open_splits is None:
            open_splits = []

        if close_splits is None:
            close_splits = []

        if special_separators is None:
            special_separators = []

        segments = []
        segment = ""

        l_2 = ""
        index = 0

        while index < len(text):
            l_1 = l_2
            l_2 = text[index]

            if l_2 in open_splits and (l_1 == '' or l_1 == separator or separator == ""):
                open_split_index = open_splits.index(l_2)

                segment = MongoQueryParser._do_encapsulated_split(text[index:], sep_init=l_2, sep_end=close_splits[open_split_index], include_separators=l_2 in special_separators)
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

    @staticmethod
    def _do_encapsulated_split(text:str, sep_init="(", sep_end=")", include_separators=False):
        """
        Splits by text between tokens.
        :param text: text to split
        :param sep_init: token that specifies the start of the segment
        :param sep_end: token that specifies the end of the segment
        :param include_separators: boolean flag that specifies if the token separators should be included in the
        segment or not.
        :return: split segments
        """
        segment = ""
        open = False
        ESCAPE_CHAR = '\\'
        previous = ""
        for index, l in enumerate(text):

            if l == sep_init and not open and previous != ESCAPE_CHAR:
                open = True

            elif l == sep_end and previous != ESCAPE_CHAR:
                break

            elif open:
                segment += l
            previous = l

        return segment if not include_separators else sep_init + segment + sep_end


    def _retrieve_ops2_tree(splits:list):
        """
        Retrieves the ops tree for second level operations.
        :return:
        """
        first_level_operators = [">", "<", "=", "!=", ">=", "<=", "eq", "!eq", "in", "%", "!%"]

        # splits should be a 3 elements list : operand1 operator operand2
        if len(splits) < 3:
            raise Exception("Invalid syntax on {}".format(" ".join(splits)))

        operator = splits[1]
        if operator not in first_level_operators:
            raise Exception("Invalid operator on {}".format(" ".join(splits)))

        previous_oper = splits[0:1]
        next_oper = splits[2:]
        return [operator, previous_oper, next_oper]

    @staticmethod
    def _retrieve_ops_tree(splits:list, open_splits:list=None, close_splits:list=None):
        """
        Retrieves the ops tree from a split query.
        :param splits: split segments from the query.
        :param open_splits: token that specifies the open of the splits, forwarded to the 'do_split' function.
        :param close_splits: token that specifies the close of the splits, forwarded to the 'do_split' function.
        :return:
        """
        second_level_operators = ["and", "or"]

        operators = {operator: [] for operator in second_level_operators+ [None]}

        previous_index = 0
        last_operand = None

        for index, operand in enumerate(splits):
            if operand in operators:
                last_operand = operand
                previous_oper = splits[previous_index:index]

                if len(previous_oper) > 1:
                    previous_oper = MongoQueryParser._retrieve_ops2_tree(previous_oper)

                elif len(previous_oper) == 1 and previous_oper[0][0] != "[":
                        previous_oper = MongoQueryParser._retrieve_ops_tree(MongoQueryParser._do_split(previous_oper[0].strip(), open_splits=open_splits, close_splits=close_splits))

                operators[operand].append(previous_oper)
                previous_index = index + 1

        if previous_index < index + 1:
            previous_oper = splits[previous_index:]

            if len(previous_oper) > 1:
                previous_oper = MongoQueryParser._retrieve_ops2_tree(previous_oper)
            else:
                previous_oper = MongoQueryParser._retrieve_ops_tree(MongoQueryParser._do_split(previous_oper[0], open_splits=open_splits, close_splits=close_splits))

            operators[last_operand].append(previous_oper)

        return operators

    @staticmethod
    def _to_mongo_query_list(ops_list:list):
        """
        Converts a list of operations into a Mongo query.
        This method is recursively called by the method _to_mongo_query_dict() if it finds any

        :param ops_list: list of mongo operations to convert.
        :return: Mongo query.
        """

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
            result[operand1] = {"$gt": float(operand2)-1}
        elif operation == "%":
            result[operand1] = {"$regex": operand2}
        elif operation == "!%":
            result[operand1] = {"$not": re.compile(operand2)}
        elif operation == "in":
            splits = MongoQueryParser._do_split(operand2[1:-1], separator="", open_splits=["'"], close_splits=["'"])
            if len(splits) == 1:
                try:
                    splits = [float(x) for x in MongoQueryParser._do_split(operand2[1:-1], separator=",")]
                except:
                    pass

            result[operand1] = {"$in": splits}

        return result

    @staticmethod
    def _to_mongo_query_dict(ops_dict):
        """
        Converts an ops tree into a Mongo query.
        :param ops_dict: Ops tree, extracted from the _retrieve_ops_tree()
        :return: Mongo query.
        """

        ors = ops_dict['or']
        ands = ops_dict['and']
        base = ops_dict[None]

        result = {}
        if len(ors) > 0:
            list_ors = []

            for op in ors:
                if type(op) is list:
                    l = MongoQueryParser._to_mongo_query_list(op)
                elif type(op) is dict:
                    l = MongoQueryParser._to_mongo_query_dict(op)

                list_ors += [l]

            result['$or'] = list_ors

        if len(ands) > 0:
            list_ands = []
            for op in ands:
                if type(op) is list:
                    l = MongoQueryParser._to_mongo_query_list(op)
                elif type(op) is dict:
                    l = MongoQueryParser._to_mongo_query_dict(op)
                list_ands += [l]

            if len(result) > 0:
                result['$or'] += list_ands
            else:
                result['$and'] = list_ands

        if len(base) > 0:
            result = MongoQueryParser._to_mongo_query_list(base[0])

        return result



