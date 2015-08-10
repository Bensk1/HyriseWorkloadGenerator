import linecache

class QueryClass:

    def __init__(self, description, table, columns, values, tableDirectory):
        self.description = description
        self.table = table
        self.columns = columns
        self.values = values
        self.tableDirectory = tableDirectory
        self.datatypes = []
        self.queryJson = self.createQueryJson()

    def createQueryJson(self):
        self.determineDatatypes()

    def determineDatatypes(self):
        for column in self.columns:
            tableFile = "%s/%s.tbl" % (self.tableDirectory, self.table)
            datatypes = linecache.getline(tableFile, 2)
            self.datatypes.append(datatypes.split('|')[column])

    def execute(self, batches, concurrencyLevel):
        # print requests,
        print "%i %i" % (batches, concurrencyLevel)
        # return concurrencyLevel