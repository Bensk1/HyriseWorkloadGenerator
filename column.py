DATATYPES = {
    "INTEGER": 0,
    "STRING": 2
}

class Column():

    def __init__(self, column, predicateType, value, datatype):
        self.column = column
        self.predicateType = predicateType
        self.value = value
        self.datatype = DATATYPES[datatype]