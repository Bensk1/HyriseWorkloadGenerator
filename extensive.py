import config
import sys

from random import seed
from table import Table
from tableLoader import TableLoader

# For testing purposes, uncomment for random tables
seed(1238585430324)

tableDirectory = sys.argv[1]

tableLoader = TableLoader(tableDirectory)
tableLoader.loadTables()

tables = []
for tableName in tableLoader.getTableNames():
    tables.append(Table(tableDirectory, tableName))
