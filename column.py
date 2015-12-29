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

    def debugPrint(self):
        print "Column %i predicateType %s value %s datatype %i" % (self.column, self.predicateType, self.value, self.datatype)