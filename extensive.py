import config
import sys

from indexEngine import IndexEngine
from querySender import QuerySender
from random import randint, seed, shuffle, uniform
from table import Table
from tableLoader import TableLoader

QUERIES_PER_DAY = 1000
RANDOM_PERCENTAGE_PER_DAY = 0.05
DAYS = 20
NOISE_FACTOR = 0.03


class Runner:

    def __init__(self, tableDirectory):
        self.currentDay = 1
        self.tableDirectory = tableDirectory

        self.tableLoader = TableLoader(tableDirectory)
        self.tableLoader.loadTables()

        self.querySender = QuerySender()
        self.indexEngine = IndexEngine()

        self.tables = []
        for tableName in self.tableLoader.getTableNames():
            self.tables.append(Table(self.tableDirectory, tableName))

        self.determineBoostPeriod()
        self.periodActive = False

    def addPeriodicQueries(self, queries):
        period = config.config["weeklyPeriodicQuery"]

        if self.currentDay >= period["startingFromDay"]:
            if (self.currentDay % 7) == period["startDay"]:
                self.periodActive = True
                self.remainingPeriodDuration = period["duration"]

            if self.periodActive:
                self.remainingPeriodDuration -= 1
                if self.remainingPeriodDuration == 0:
                    self.periodActive = False
                for i in range(50):
                    queries.append(self.tables[0].mediumQuery)


    def addRandomQueries(self, numberOfQueries, queries):
        for i in range(numberOfQueries):
            randomTable = randint(0, len(self.tables) - 1)
            randomQuery = randint(0, config.config["randomQueriesPerTable"] - 1)
            queries.append(self.tables[randomTable].randomQueries[randomQuery])

    def boostTableShares(self, tableShares, queriesToday):
        if self.currentDay % self.boostPeriod == 1:
            self.determineBoostTables()
            self.determineBoostPeriod()

        for boostIndex, value in zip(self.boostTables, config.config["boostValues"]):
            tableShares[boostIndex] = int(tableShares[boostIndex] + value * queriesToday)

    def determineBoostPeriod(self):
        self.boostPeriod = randint(3, 4)

    def prepareDay(self):
        queriesToday = self.noiseNumberOfQueries(QUERIES_PER_DAY)
        randomQueriesToday = int(queriesToday * RANDOM_PERCENTAGE_PER_DAY)
        sharedUsualQueries = 1.0 - RANDOM_PERCENTAGE_PER_DAY - reduce(lambda x, y: x + y, config.config["boostValues"])
        usualQueries = int(queriesToday * sharedUsualQueries)

        tableShares = [usualQueries / len(self.tables)] * len(self.tables)
        tableShares = self.noiseTableShares(tableShares)
        # print reduce(lambda x,y: x + y, tableShares)
        self.boostTableShares(tableShares, queriesToday)

        # print reduce(lambda x,y: x + y, tableShares)

        queries = self.prepareQueries(tableShares)

        self.addPeriodicQueries(queries)

        self.addRandomQueries(randomQueriesToday, queries)
        shuffle(queries)

        self.querySender.sendQueries(queries)

        if config.config["selfTunedIndexSelection"]:
            self.indexEngine.triggerIndexOptimization()

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

    def prepareQueries(self, tableShares):
        queries = []
        for tableShare, table in zip(tableShares, self.tables):
            queries += [table.smallQuery] * (int(tableShare * config.config["queryShare"][0]))
            queries += [table.mediumQuery] * (int(tableShare * config.config["queryShare"][1]))
            queries += [table.largeQuery] * (int(tableShare * config.config["queryShare"][2]))

        return queries

# For testing purposes, uncomment for random tables
seed(1238585430324)

runner = Runner(sys.argv[1])

for i in range(DAYS):
    runner.prepareDay()