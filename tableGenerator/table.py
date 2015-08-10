from random import randint
from random import choice
import string
import os

class Table:

    def __init__(self, name, rows, columns, stringsForEachInt, stringLength, uniqueValues, path):
        self.name = name
        self.rows = rows
        self.columns = columns
        self.stringsForEachInt = stringsForEachInt + 1
        self.minStringLength = stringLength[0]
        self.maxStringLength = stringLength[1]
        self.uniqueValues = self.normalizeUniqueValues(uniqueValues, self.columns)

        self.checkAndCreatePath(path)
        self.outputFile = open("%s/%s.tbl" % (path, self.name), "w")

    def checkAndCreatePath(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def normalizeUniqueValues(self, uniqueValues, columns):
        if isinstance(uniqueValues, list):
            return uniqueValues
        else:
            return [uniqueValues] * columns

    def buildTableHeader(self):
        self.buildColumnNames()
        self.buildDataTypes()
        self.buildPartitioning()
        self.buildHeaderBoundary()

    def buildColumnNames(self):
        columnNames = ""
        for column in range(0, self.columns):
            if column > 0:
                columnNames += "|"
            columnNames += "col_%i" % (column)

        self.outputFile.write(columnNames + "\n")

    def buildDataTypes(self):
        columnTypes = ""
        for column in range(0, self.columns):
            if column > 0:
                columnTypes += "|"
            if column % self.stringsForEachInt == 0:
                columnTypes += "INTEGER"
            else:
                columnTypes += "STRING"

        self.outputFile.write(columnTypes + "\n")

    def buildPartitioning(self):
        columnPartition = ""
        for column in range(0, self.columns):
            if column > 0:
                columnPartition += "|"
            columnPartition += "%i_C" % (column)

        self.outputFile.write(columnPartition + "\n")

    def buildHeaderBoundary(self):
        self.outputFile.write("===\n")

    def determineStringColumnLength(self):
        # None indicating that column is not a string column
        self.stringColumnLengths = []

        for column in range(0, self.columns):
            if column % self.stringsForEachInt == 0:
                self.stringColumnLengths.append(None)
            else:
                self.stringColumnLengths.append(randint(self.minStringLength, self.maxStringLength))

    def generateRandomInts(self, amount):
        startValue = randint(0, 1000000)
        values = []

        for value in range(0, amount):
            values.append(str(startValue + value))

        return values

    def generateRandomString(self, length, chars=string.ascii_uppercase + string.digits + string.ascii_lowercase):
        return ''.join(choice(chars) for _ in range(length))

    def generateRandomStrings(self, length, amount):
        values = []

        for value in range(0, amount):
            values.append(self.generateRandomString(length))

        return values

    def generateValues(self):
        self.values = []

        for column in range(0, self.columns):
            if self.stringColumnLengths[column] == None:
                self.values.append(self.generateRandomInts(self.uniqueValues[column]))
            else:
                self.values.append(self.generateRandomStrings(self.stringColumnLengths[column], self.uniqueValues[column]))

    def buildTableData(self):
        for row in range(0, self.rows):
            rowValues = ""
            for column in range(0, self.columns):
                if column > 0:
                    rowValues += "|"
                rowValues += self.values[column][randint(0, self.uniqueValues[column] - 1)]

            self.outputFile.write(rowValues + "\n")

    def build(self):
        self.buildTableHeader()
        self.determineStringColumnLength()
        self.generateValues()
        self.buildTableData()
        self.outputFile.close()