import config
import linecache

from column import Column
from jinja2 import Template
from random import randint, shuffle
from query import Query, QueryType
from subprocess import Popen, PIPE

TABLE_HEADER_SIZE = 4

class Table:

    def __init__(self, directory, name):
        self.directory = directory
        self.name = name
        self.datatypes = self.determineDatatypes()
        self.values = self.getRandomValues()

        # Three queries with configurable size
        self.generateQueries()
        self.randomQueries = self.generateRandomQueries()

    def determineDatatypes(self):
        tableFile = "%s/%s.tbl" % (self.directory, self.name)
        datatypes = linecache.getline(tableFile, 2)
        datatypes = datatypes.split('|')

        # Last column is followed by \n, throw that away
        datatypes[-1] = datatypes[-1].split('\n')[0]

        return datatypes

    def generateRandomQuery(self, queryType):
        columns = self.getRandomColumnSelection(queryType)
        compoundExpressions = self.parseCompoundExpressions(self.generateRandomQueryCompoundExpressions(columns, queryType))
        columnObjects = []

        for column in columns:
            columnObjects.append(Column(column, "EQ", self.values[column], self.datatypes[column]))

        if config.config["loadDumpedTables"]:
            tableName = "%s_dumped" % (self.name)
        else:
            tableName = self.name

        queryTemplateFile = "queryTemplateCompressed.json" if config.config["compressedQueries"] else "queryTemplate.json"
        with open(queryTemplateFile) as queryTemplate:
            template = Template(queryTemplate.read())
            query = {}
            query['query'] = template.render(columns = columnObjects, columnLen = len(columnObjects), table = tableName, compoundExpressions = compoundExpressions, compoundExpressionLen = len(compoundExpressions))
            query['performance'] = "true"
            return Query(QueryType.random, query)

    def generateRandomQueries(self):
        queries = []

        queries.append(self.generateRandomQuery(QueryType.small))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.medium))
        queries.append(self.generateRandomQuery(QueryType.small))
        queries.append(self.generateRandomQuery(QueryType.medium))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.medium))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.large))

        queries.append(self.generateRandomQuery(QueryType.medium))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.small))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.small))
        queries.append(self.generateRandomQuery(QueryType.medium))
        queries.append(self.generateRandomQuery(QueryType.large))
        queries.append(self.generateRandomQuery(QueryType.medium))

        return queries


    def generateQueries(self):
        self.smallQuery = self.generateSmallQuery()
        self.mediumQuery = self.generateMediumQuery()
        self.largeQuery = self.generateLargeQuery()

    def generateQuery(self, numberOfAttributes, compoundExpressions):
        columnObjects = []

        for attribute in range(numberOfAttributes):
            columnObjects.append(Column(attribute, "EQ", self.values[attribute], self.datatypes[attribute]))

        if config.config["loadDumpedTables"]:
            tableName = "%s_dumped" % (self.name)
        else:
            tableName = self.name

        queryTemplateFile = "queryTemplateCompressed.json" if config.config["compressedQueries"] else "queryTemplate.json"
        with open(queryTemplateFile) as queryTemplate:
            template = Template(queryTemplate.read())
            query = {}
            query['query'] = template.render(columns = columnObjects, columnLen = len(columnObjects), table = tableName, compoundExpressions = compoundExpressions, compoundExpressionLen = len(compoundExpressions))
            query['performance'] = "true"
            return query

    def generateSmallQuery(self):
        compoundExpressions = self.parseCompoundExpressions(config.config["smallQueriesCompoundExpression"])

        return Query(QueryType.small, self.generateQuery(config.config["smallQueriesAttributes"], compoundExpressions))

    def generateMediumQuery(self):
        compoundExpressions = self.parseCompoundExpressions(config.config["mediumQueriesCompoundExpression"])

        return Query(QueryType.medium, self.generateQuery(config.config["mediumQueriesAttributes"], compoundExpressions))

    def generateLargeQuery(self):
        compoundExpressions = self.parseCompoundExpressions(config.config["largeQueriesCompoundExpression"])

        return Query(QueryType.large, self.generateQuery(config.config["largeQueriesAttributes"], compoundExpressions))

    def getRandomColumnSelection(self, queryType):
        columns = range(len(self.values))
        shuffle(columns)

        return columns[:config.config["randomQueriesAttributes"][queryType.value]]


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

    def parseCompoundExpressions(self, compoundExpressions):
        for compoundExpression in compoundExpressions:
            compoundExpression['l'] = self.renameCompoundExpressionBranch(compoundExpression['l'])
            compoundExpression['r'] = self.renameCompoundExpressionBranch(compoundExpression['r'])

        return compoundExpressions

    def renameCompoundExpressionBranch(self, compoundExpressionBranch):
        if type(compoundExpressionBranch) is int:
            if config.config["compressedQueries"]:
                compoundExpressionBranch = "sC%i" % (compoundExpressionBranch)
            else:
                compoundExpressionBranch = "scanColumn%i" % (compoundExpressionBranch)

        return compoundExpressionBranch

    def generateRandomQueryCompoundExpressions(self, columns, queryType):

        if queryType == QueryType.small:
            return [{
                "name": "0and1",
                "type": "and",
                "l": columns[0],
                "r": columns[1]
            },
            {
                "name": "0and1and2",
                "type": "and",
                "l": "0and1",
                "r": columns[2]
            }]

        elif queryType == QueryType.medium:
            return [{
                "name": "0and1",
                "type": "and",
                "l": columns[0],
                "r": columns[1]
            },
            {
                "name": "2and3",
                "type": "and",
                "l": columns[2],
                "r": columns[3]
            },
            {
                "name": "0and1and4",
                "type": "and",
                "l": "0and1",
                "r": columns[4]
            },
            {
                "name": "0and1and4and2and3",
                "type": "and",
                "l": "0and1and4",
                "r": "2and3"
            }]
        else:
            return [{
                "name": "0and1",
                "type": "and",
                "l": columns[0],
                "r": columns[1]
            },
            {
                "name": "2and3",
                "type": "and",
                "l": columns[2],
                "r": columns[3]
            },
            {
                "name": "4and5",
                "type": "and",
                "l": columns[4],
                "r": columns[5]
            },
            {
                "name": "6and7",
                "type": "and",
                "l": columns[6],
                "r": columns[7]
            },
            {
                "name": "0and1and8",
                "type": "and",
                "l": "0and1",
                "r": columns[8]
            },
            {
                "name": "2and3and4and5",
                "type": "and",
                "l": "2and3",
                "r": "4and5"
            },
            {
                "name": "0and1and8and6and7",
                "type": "and",
                "l": "0and1and8",
                "r": "6and7"
            },
            {
                "name": "0and1and8and6and7and2and3and4and5",
                "type": "and",
                "l": "0and1and8and6and7",
                "r": "2and3and4and5"
            }]