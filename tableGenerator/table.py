from random import choice, randint, shuffle
import string
import os

class Table:

    def __init__(self, name, rows, columns, stringsForEachInt, stringLength, uniqueValues, path, metaDataFile):
        self.name = name
        self.rows = rows
        self.columns = columns
        self.stringsForEachInt = stringsForEachInt + 1
        self.minStringLength = stringLength[0]
        self.maxStringLength = stringLength[1]
        self.uniqueValues = self.normalizeUniqueValues(uniqueValues, self.columns)

        self.memoryBudgetForFullIndexation = self.calculateMemoryBudget()

        self.checkAndCreatePath(path)
        self.outputFile = open("%s/%s.tbl" % (path, self.name), "w")
        self.metaDataFile = metaDataFile

    def checkAndCreatePath(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def calculateMemoryBudget(self):
        memoryBudget = 0

        for uniqueValue in self.uniqueValues:
            memoryBudget += 8 * (uniqueValue + 1)
            memoryBudget += 8 * self.rows
            memoryBudget += 8 + 24 + 24 + 24

        return memoryBudget


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

    def generateValueOrder(self):
        self.valueOrder = range(self.rows)
        shuffle(self.valueOrder)

    def buildTableData(self):
        for row in range(0, self.rows):
            rowValues = ""

            for column in range(0, self.columns):
                if column > 0:
                    rowValues += "|"
                rowValues += self.values[column][self.valueOrder[row] % self.uniqueValues[column]]

            self.outputFile.write(rowValues + "\n")

    def writeTableMetaData(self):
        self.metaDataFile.write("%s rows: %i\n" % (self.name, self.rows))

        for column in range(0, self.columns):
            try:
                int(self.values[column][0])
                self.metaDataFile.write("Column: %i min %s max %s fullIndexMemoryBudget %i \n" % (column, self.values[column][0], self.values[column][self.uniqueValues[column] - 1], self.memoryBudgetForFullIndexation))
            except:
                pass

    def build(self):
        self.buildTableHeader()
        self.determineStringColumnLength()
        self.generateValues()
        self.generateValueOrder()
        self.buildTableData()
        self.writeTableMetaData()
        self.outputFile.close()

        return self.memoryBudgetForFullIndexation