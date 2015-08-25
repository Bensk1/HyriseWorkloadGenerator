import linecache
import os
import numpy as np
from random import randint
from subprocess import Popen, PIPE
from jinja2 import Template
from column import Column

TABLE_HEADER_SIZE = 4
STATISTICAL_FUNCTIONS = {
    'mean': np.mean,
    'min': np.min,
    'max': np.max,
    'median': np.median,
    'percentile25': lambda x: np.percentile(x, 25),
    'percentile75': lambda x: np.percentile(x, 75)
}

class QueryClass:

    def __init__(self, description, table, columns, compoundExpressions, values, compressed, tableDirectory, days):
        self.description = description
        self.table = table
        self.columns = columns
        self.tableDirectory = tableDirectory
        self.compressed = compressed
        self.compoundExpressions = self.parseCompoundExpressions(compoundExpressions)
        self.values = self.parseValues(values)
        self.datatypes = []
        self.statistics = [[] for i in range(days)]
        self.period = 0
        self.activeToday = True

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

    def renameCompoundExpressionBranch(self, compoundExpressionBranch):
        if type(compoundExpressionBranch) is int:
            if self.compressed:
                compoundExpressionBranch = "sC%i" % (compoundExpressionBranch)
            else:
                compoundExpressionBranch = "scanColumn%i" % (compoundExpressionBranch)

        return compoundExpressionBranch

    def parseCompoundExpressions(self, compoundExpressions):
        for compoundExpression in compoundExpressions:
            compoundExpression['l'] = self.renameCompoundExpressionBranch(compoundExpression['l'])
            compoundExpression['r'] = self.renameCompoundExpressionBranch(compoundExpression['r'])

        return compoundExpressions

    def getRowsInTable(self):
        wc = Popen(["wc -l %s/%s.tbl" % (self.tableDirectory, self.table)], shell = True, stdout = PIPE)
        (output, err) = wc.communicate()
        wc.wait()

        rows = int(output.split(' ')[0]) - TABLE_HEADER_SIZE
        return rows

    def calculateStatistics(self):
        statistics = {}

        for key in STATISTICAL_FUNCTIONS:
            statistics[key] = map(lambda x: STATISTICAL_FUNCTIONS[key](x) if len(x) > 0 else 0, self.statistics)

        return statistics

    def addStatistics(self, day, values):
        self.statistics[day - 1] += values

    def checkAndCreateQueryFolder(self):
        if not os.path.exists("queries"):
            os.makedirs("queries")

    def createQueryJson(self):
        self.determineDatatypes()

        columnObjects = []
        for i, (column, value, datatype) in enumerate(zip(self.columns, self.values, self.datatypes)):
            colObject = Column(column, value, datatype)
            columnObjects.append(colObject)

        self.determineQueryFileName()

        queryTemplateFile = "queryTemplateCompressed.json" if self.compressed else "queryTemplate.json"
        with open(queryTemplateFile) as queryTemplate:
            template = Template(queryTemplate.read())
            return template.render(columns = columnObjects, columnLen = len(columnObjects), table = self.table, compoundExpressions = self.compoundExpressions, compoundExpressionLen = len(self.compoundExpressions))

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

        # Last column is followed by \n, throw that away
        self.datatypes[-1] = self.datatypes[-1].split('\n')[0]