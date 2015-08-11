import linecache
import os
import re
from random import randint
from subprocess import Popen, PIPE
from jinja2 import Template
from column import Column

TABLE_HEADER_SIZE = 4

class QueryClass:

    def __init__(self, description, table, columns, values, tableDirectory):
        self.description = description
        self.table = table
        self.columns = columns
        self.tableDirectory = tableDirectory
        self.values = self.parseValues(values)
        self.datatypes = []
        self.statistics = []

        self.checkAndCreateQueryFolder()
        self.queryJson = self.createQueryJson()
        self.writeQueryFile()

    def parseValues(self, values):
        if values <> 'auto':
            return values

        rows = self.getRowsInTable()
        rowNumber = randint(1 + TABLE_HEADER_SIZE , rows + TABLE_HEADER_SIZE)
        tableFile = "%s/%s.tbl" % (self.tableDirectory, self.table)
        randomRow = linecache.getline(tableFile, rowNumber)

        columnsOfRow = randomRow.split('|')
        values = []

        for column in self.columns:
            values.append(columnsOfRow[column])

        return values


    def getRowsInTable(self):
        wc = Popen(["wc -l %s/%s.tbl" % (self.tableDirectory, self.table)], shell = True, stdout = PIPE)
        (output, err) = wc.communicate()
        wc.wait()

        rows = int(output.split(' ')[0]) - TABLE_HEADER_SIZE
        return rows

    def showStatistics(self):
        print "%s: %s" % (self.description, self.statistics)

    def addStatistic(self, value):
        self.statistics.append(value)

    def checkAndCreateQueryFolder(self):
        if not os.path.exists("queries"):
            os.makedirs("queries")

    def createQueryJson(self):
        self.determineDatatypes()

        columnObjects = []
        for i, (column, value, datatype) in enumerate(zip(self.columns, self.values, self.datatypes)):
            last = True if i == len(self.columns) - 1 else False
            colObject = Column(column, value, datatype, last)
            columnObjects.append(colObject)

        self.determineQueryFileName()

        with open("queryTemplate.json") as queryTemplate:
            template = Template(queryTemplate.read())
            return template.render(columns = columnObjects, columnLen = len(columnObjects), table = self.table)

    def determineQueryFileName(self):
        self.queryFilePath = "queries/%s.json" % (self.description)

    def writeQueryFile(self):
        queryFile = open(self.queryFilePath, 'w')
        queryFile.write(self.queryJson)
        queryFile.close()

    def determineDatatypes(self):
        for column in self.columns:
            tableFile = "%s/%s.tbl" % (self.tableDirectory, self.table)
            datatypes = linecache.getline(tableFile, 2)
            self.datatypes.append(datatypes.split('|')[column])

    def execute(self, batches, concurrencyLevel):
        requests = batches * concurrencyLevel

        if requests <= 0:
            return None

        ab = Popen(["ab -q -k -n %i -c %i -r -T \"application/x-www-form-urlencoded\" -p \"%s\" 127.0.0.1:5000/jsonQuery/" % (requests, concurrencyLevel, self.queryFilePath)], shell = True, stdout = PIPE)
        (output, err) = ab.communicate()
        ab.wait()
        timeTakenString = output.split('\n')[15]
        timeTaken = float(re.findall("\d+.\d+", timeTakenString)[0])
        timeTakenInMs = timeTaken / requests * 1000

        print "%s: %s (%fms per request)" % (self.description, timeTaken, timeTakenInMs)

        return timeTakenInMs