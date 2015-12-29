import config
import sys

from random import randint, seed, uniform
from table import Table
from tableLoader import TableLoader

QUERIES_PER_DAY = 10000
RANDOM_PERCENTAGE_PER_DAY = 0.05
SHARED_USUAL_QUERIES = 0.6
DAYS = 1
NOISE_FACTOR = 0.01


class Runner:

    def __init__(self, tableDirectory):
        self.currentDay = 1
        self.tableDirectory = tableDirectory
        self.tableLoader = TableLoader(tableDirectory)
        self.tableLoader.loadTables()

        self.tables = []
        for tableName in self.tableLoader.getTableNames():
            self.tables.append(Table(self.tableDirectory, tableName))

    def boostTableShares(self, tableShares):
        if self.currentDay % 90 == 1:
            self.determineBoostTables()

        for boostIndex, value in zip(self.boostTables, config.config["boostValues"]):
            tableShares[boostIndex] = int(tableShares[boostIndex] * value)

    def calculateDay(self):
        queriesToday = self.noiseNumberOfQueries(QUERIES_PER_DAY)
        randomQueriesToday = int(queriesToday * RANDOM_PERCENTAGE_PER_DAY)
        usualQueries = int(queriesToday - randomQueriesToday)

        tableShares = [usualQueries * SHARED_USUAL_QUERIES / len(self.tables)] * len(self.tables)
        tableShares = self.noiseTableShares(tableShares)

        self.boostTableShares(tableShares)

        print tableShares
        self.currentDay += 1

    def determineBoostTables(self):
        self.boostTables = []

        for i in range(len(config.config["boostValues"])):
            self.boostTables.append(randint(0, len(self.tables) - 1))

    def noiseNumberOfQueries(self, numberOfQueries):
        multiplier = uniform(-NOISE_FACTOR, NOISE_FACTOR)

        return int(numberOfQueries * (1 + multiplier))

    def noiseTableShares(self, tableShares):
        multipliers = []
        numberOfMultipliers = len(self.tables) / 2 if len(self.tables) % 2 == 0 else len(self.tables) / 2 + 1
        for i in range(numberOfMultipliers):
            multipliers.append(uniform(-NOISE_FACTOR, NOISE_FACTOR))
            multipliers.append(multipliers[-1] * -1)

        tableShares = map(lambda tableShare, multiplier: int(tableShare * (1 + multiplier)), tableShares, multipliers[:len(tableShares)])
        return tableShares


# For testing purposes, uncomment for random tables
seed(1238585430324)

runner = Runner(sys.argv[1])

for i in range(DAYS):
    runner.calculateDay()