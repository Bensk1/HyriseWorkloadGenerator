import sys
import json
from multiprocessing import Pool
from table import Table
from random import seed

# Multi-Threading leads always to different tables
MULTITHREADED = False


def buildTable(tableConfig):
    table = Table(tableConfig['name'], tableConfig['rows'], tableConfig['columns'], tableConfig['stringsForEachInt'], tableConfig['stringLength'], tableConfig['uniqueValues'], outputDirectory, metaDataFile)
    return table.build()

def buildTables():
    global configFile

    with open(configFile) as configFile:
        tableConfigs = json.load(configFile)

    # For testing purposes, uncomment for random tables
    seed(1238585430324)

    if MULTITHREADED:
        threadPool = Pool(len(tableConfigs))

        memoryBudgets = threadPool.map(buildTable, tableConfigs, 1)
        overallMemoryBudget = reduce(lambda x, y: x + y, memoryBudgets)
    else:
        overallMemoryBudget = 0

        for tableConfig in tableConfigs:
            overallMemoryBudget += buildTable(tableConfig)

    metaDataFile.write("Overall memory budget: %i" % (overallMemoryBudget))
    metaDataFile.close()

if len(sys.argv) <> 3:
    print "Usage: python generator.py config.json outputDirectory"
    sys.exit()
else:
    configFile = sys.argv[1]
    outputDirectory = sys.argv[2]
    metaDataFile = open("%s/metadata" % (outputDirectory), "w")
    buildTables()