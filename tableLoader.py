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

    def getTableNames(self):
        tableNames = []
        filenames = glob.glob("%s/*.tbl" % (self.directory))

        for filename in filenames:
            filename = filename.split(".tbl")[0]
            tableNames.append(filename.split('/')[-1])

        tableNames.sort()
        return tableNames

    def loadTables(self):
        print "Load all tables in directory: %s" % (self.directory)

        tableNames = self.getTableNames()

        threadPool = Pool(len(tableNames))
        threadPool.map(self.buildAndSendRequest, tableNames)
        threadPool.close()
        threadPool.join()

        print "Succesfully loaded %i tables" % (len(tableNames))