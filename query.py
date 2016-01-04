from enum import Enum

class QueryType(Enum):
    small = 0
    medium = 1
    large = 2
    random = 3

class Query:

    def __init__(self, queryType, text):
        self.queryType = queryType
        self.text = text
