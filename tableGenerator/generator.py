import sys
import json
from table import Table
from random import seed

def buildTables(configFile, outputDirectory):
    with open(configFile) as configFile:
        tableConfigs = json.load(configFile)

    metaDataFile = open("%s/metadata" % (outputDirectory), "w")

    # For testing purposes, uncomment for random tables
    seed(1238585430324)

    overallMemoryBudget = 0

    for tableConfig in tableConfigs:
        table = Table(tableConfig['name'], tableConfig['rows'], tableConfig['columns'], tableConfig['stringsForEachInt'], tableConfig['stringLength'], tableConfig['uniqueValues'], outputDirectory, metaDataFile)
        overallMemoryBudget += table.build()

    metaDataFile.write("Overall memory budget: %i" % (overallMemoryBudget))
    metaDataFile.close()

if len(sys.argv) <> 3:
    print "Usage: python generator.py config.json outputDirectory"
    sys.exit()
else:
    buildTables(sys.argv[1], sys.argv[2])