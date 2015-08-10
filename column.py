DATATYPES = {
    "INTEGER": 0,
    "STRING": 2
}

class Column():

    def __init__(self, column, value, datatype, last):
        self.column = column
        self.value = value
        self.datatype = DATATYPES[datatype]
        self.last = last