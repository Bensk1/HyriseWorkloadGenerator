import sys

from table import Table
from tableLoader import TableLoader

tableDirectory = sys.argv[1]

tableLoader = TableLoader(tableDirectory)
tableLoader.loadTables()

tables = []
for tableName in tableLoader.getTableNames():
    tables.append(Table(tableName))
