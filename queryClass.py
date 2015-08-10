import linecache
import os
from subprocess import Popen, PIPE
from jinja2 import Template
from column import Column

class QueryClass:

    def __init__(self, description, table, columns, values, tableDirectory):
        self.description = description
        self.table = table
        self.columns = columns
        self.values = values
        self.tableDirectory = tableDirectory
        self.datatypes = []

        self.checkAndCreateQueryFolder()
        self.queryJson = self.createQueryJson()
        self.writeQueryFile()

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
        a = Popen(["ab -k -n %i -c %i -r -T \"application/x-www-form-urlencoded\" -p \"%s\" 127.0.0.1:5000/jsonQuery/" % (batches * concurrencyLevel, concurrencyLevel, self.queryFilePath)], shell = True, stdout = PIPE)
        (output, err) = a.communicate()
        # print output
        exit_code = a.wait()