import config
import linecache

from column import Column
from jinja2 import Template
from random import randint
from subprocess import Popen, PIPE

TABLE_HEADER_SIZE = 4

class Table:

    def __init__(self, directory, name):
        self.directory = directory
        self.name = name
        self.datatypes = self.determineDatatypes()
        self.values = self.getRandomValues()

        # Three queries with configurable size
        self.queries = self.generateQueries()
        self.randomQueries = []

        print self.queries

    def determineDatatypes(self):
        tableFile = "%s/%s.tbl" % (self.directory, self.name)
        datatypes = linecache.getline(tableFile, 2)
        datatypes = datatypes.split('|')

        # Last column is followed by \n, throw that away
        datatypes[-1] = datatypes[-1].split('\n')[0]

        return datatypes

    def generateQueries(self):
        queries = []

        queries.append(self.generateSmallQuery())
        queries.append(self.generateMediumQuery())
        queries.append(self.generateLargeQuery())

        return queries

    def generateQuery(self, numberOfAttributes, compoundExpressions):
        columnObjects = []

        for attribute in range(numberOfAttributes):
            columnObjects.append(Column(attribute, "EQ", self.values[attribute], self.datatypes[attribute]))

        queryTemplateFile = "queryTemplateCompressed.json" if config.config["compressedQueries"] else "queryTemplate.json"
        with open(queryTemplateFile) as queryTemplate:
            template = Template(queryTemplate.read())
            return template.render(columns = columnObjects, columnLen = len(columnObjects), table = self.name, compoundExpressions = compoundExpressions, compoundExpressionLen = len(compoundExpressions))

    def generateSmallQuery(self):
        return self.generateQuery(config.config["smallQueriesAttributes"], config.config["smallQueriesCompoundExpression"])

    def generateMediumQuery(self):
        return self.generateQuery(config.config["mediumQueriesAttributes"], config.config["mediumQueriesCompoundExpression"])

    def generateLargeQuery(self):
        queryFile = open("asd.json", 'w')
        queryFile.write(self.generateQuery(config.config["largeQueriesAttributes"], config.config["largeQueriesCompoundExpression"]))
        queryFile.close()
        return self.generateQuery(config.config["largeQueriesAttributes"], config.config["largeQueriesCompoundExpression"])

    def getRandomValues(self):
        rows = self.getRowsInTable()
        rowNumber = randint(1 + TABLE_HEADER_SIZE , rows + TABLE_HEADER_SIZE)
        tableFile = "%s/%s.tbl" % (self.directory, self.name)
        randomRow = linecache.getline(tableFile, rowNumber)

        columnsOfRow = randomRow.split('|')

        # Last column is followed by \n, throw that away
        columnsOfRow[-1] = columnsOfRow[-1].split('\n')[0]

        return columnsOfRow

    def getRowsInTable(self):
        wc = Popen(["wc -l %s/%s.tbl" % (self.directory, self.name)], shell = True, stdout = PIPE)
        (output, err) = wc.communicate()
        wc.wait()

        rows = int(output.split(' ')[0]) - TABLE_HEADER_SIZE
        return rows