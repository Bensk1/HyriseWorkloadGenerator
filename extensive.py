import config
import sys

from random import uniform, seed
from table import Table
from tableLoader import TableLoader

QUERIES_PER_DAY = 1000
RANDOM_PERCENTAGE_PER_DAY = 0.05


class Runner:

    def __init__(self, tableDirectory):
        self.tableDirectory = tableDirectory
        self.tableLoader = TableLoader(tableDirectory)
        self.tableLoader.loadTables()

        self.tables = []
        for tableName in self.tableLoader.getTableNames():
            self.tables.append(Table(self.tableDirectory, tableName))

    def applyNoise(self, numberOfQueries):
        multiplier = uniform(-0.05, 0.05)

        return int(numberOfQueries * (1 + multiplier))

    def calculateDay(self):
        queriesToday = self.applyNoise(QUERIES_PER_DAY)
        randomQueriesToday = int(queriesToday * RANDOM_PERCENTAGE_PER_DAY)
        usualQueries = int(queriesToday - randomQueriesToday)

        print queriesToday, randomQueriesToday, usualQueries



# For testing purposes, uncomment for random tables
seed(1238585430324)

runner = Runner(sys.argv[1])

runner.calculateDay()