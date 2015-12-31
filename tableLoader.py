import glob
import requests
import util
from multiprocessing import Pool

class TableLoader:
    
    def __init__(self, directory):
        self.directory = directory

    def buildAndSendRequest(self, tableName):
        loadTableRequest = self.buildLoadTableRequest(tableName)
        requests.post("http://localhost:5000/jsonQuery", data = loadTableRequest)

    def buildAndSendRequestDumped(self, tableName):
        loadDumpedTableRequest = self.buildLoadTableRequestDumped(tableName)
        requests.post("http://localhost:5000/jsonQuery", data = loadDumpedTableRequest)

    def buildLoadTableRequest(self, tableName):
            loadTableRequest = {'query': '{\
                "operators": {\
                    "loadTable": {\
                        "type" : "GetTable",\
                        "name" : "%s"\
                    },\
                    "NoOp": {\
                        "type" : "NoOp"\
                    }\
                },\
                "edges" : [\
                    ["loadTable", "NoOp"]\
                ]\
            }' % (tableName)}

            return loadTableRequest

    def buildLoadTableRequestDumped(self, tableName):
            loadTableRequest = {'query': '{\
                "operators": {\
                    "loadDumpedTable": {\
                        "type" : "LoadDumpedTable",\
                        "name" : "%s_dumped"\
                    },\
                    "NoOp": {\
                        "type" : "NoOp"\
                    }\
                },\
                "edges" : [\
                    ["loadDumpedTable", "NoOp"]\
                ]\
            }' % (tableName)}

            return loadTableRequest

    def getTableNames(self):
        tableNames = []
        filenames = glob.glob("%s/*.tbl" % (self.directory))

        for filename in filenames:
            filename = filename.split(".tbl")[0]
            tableNames.append(filename.split('/')[-1])

        tableNames.sort()
        return tableNames

    def loadTables(self, dumped):
        print "Load all tables in directory: %s" % (self.directory)

        tableNames = self.getTableNames()

        threadPool = Pool(len(tableNames))

        if dumped:
            threadPool.map(self.buildAndSendRequestDumped, tableNames)
        else:
            threadPool.map(self.buildAndSendRequest, tableNames)

        threadPool.close()
        threadPool.join()

        print "Succesfully loaded %i tables" % (len(tableNames))