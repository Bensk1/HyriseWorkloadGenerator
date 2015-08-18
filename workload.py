# -*- coding: utf-8 -*-

import sys
import json
import types
import copy_reg
import requests
import glob
import time
import requests
from multiprocessing import Pool
from queryClass import QueryClass
from queryClassDistribution import QueryClassDistribution

QUERY_DIVISOR = 100
DISTRIBUTION_DIVISOR = 10
TICK_MS = 0.1
QUERIES_PER_TICK = 10

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, _pickle_method)

def simulateDay(dayInformation):
    ticksPerDay = dayInformation[0]
    queryFilePath = dayInformation[1]
    nextTick = time.time()

    queryFile = open(queryFilePath, "r")
    query = {}
    query['query'] = queryFile.read()

    while ticksPerDay > 0:
        r = requests.post("http://localhost:5000/jsonQuery", data=query)
        nextTick = nextTick + TICK_MS
        ticksPerDay -= 1
        # wait including drift correction
        time.sleep(nextTick - time.time())

    queryFile.close()

class Object:
    def toJSON(self):
        return json.dumps(self, default = lambda obj: obj.__dict__, indent = 2)

class Workload(Object):

    def __init__(self, days, secondsPerDay, queryClasses, queryClassDistributions, verbose, compressed, tableDirectory):
        self.days = days
        self.currentDay = 1
        self.secondsPerDay = secondsPerDay
        self.tableDirectory = tableDirectory
        self.queryClasses = self.parseQueryClasses(queryClasses, compressed, self.tableDirectory)
        self.queryClassDistributions = self.parseQueryClassDistributions(queryClassDistributions)
        self.verbose = verbose
        self.currentlyActiveQueryDistribution = None
        self.currentQueryOrder = None
        self.activeQueryClassDistributionChanged = False
        self.currentQueryBatchOrder = None
        self.ticksPerDay = int(1 / TICK_MS * self.secondsPerDay)
        self.queriesPerDay = self.ticksPerDay * QUERIES_PER_TICK

        self.loadAllTables()

        self.threadPool = Pool(QUERIES_PER_TICK)

        self.statistics = self.initializeStatistics()

    def initializeStatistics(self):
        statistics = {}
        for queryClass in self.queryClasses:
            statistics[queryClass.description] = []

        return statistics

    def getTableNames(self):
        tableNames = []
        filenames = glob.glob("%s/*.tbl" % (self.tableDirectory))

        for filename in filenames:
            filename = filename.split(".tbl")[0]
            tableNames.append(filename.split('/')[-1])

        return tableNames

    def buildAndSendRequests(self, tableName):
        loadTableRequest = self.buildLoadTableRequest(tableName)
        r = requests.post("http://localhost:5000/jsonQuery", data = loadTableRequest)

    def loadAllTables(self):
        print "Load all tables in directory: %s" % (self.tableDirectory)

        tableNames = self.getTableNames()

        threadPool = Pool(len(tableNames))
        threadPool.map(self.buildAndSendRequests, tableNames)
        threadPool.close()
        threadPool.join()

        print "Succesfully loaded %i tables" % (len(tableNames))

    def buildLoadTableRequest(self, table):
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
        }' % (table)}

        return loadTableRequest

    def parseQueryClasses(self, queryClasses, compressed, tableDirectory):
        queryClassesParsed = []

        for queryClass in queryClasses:
            queryClassesParsed.append(QueryClass(queryClass['description'], queryClass['table'], queryClass['columns'], queryClass['compoundExpressions'], queryClass['values'], compressed, tableDirectory))

        return queryClassesParsed

    def parseQueryClassDistributions(self, queryClassDistributions):
        queryClassDistributionsParsed = []

        for queryClassDistribution in queryClassDistributions:
            queryClassDistributionsParsed.append(QueryClassDistribution(queryClassDistribution['description'], queryClassDistribution['validFromDay'], queryClassDistribution['distribution']))

        return queryClassDistributionsParsed

    def determineCurrentlyActiveDistribution(self):
        activeQueryClassDistribution = None
        oldActiveQueryClassDistribution = self.currentlyActiveQueryDistribution

        for queryClassDistribution in self.queryClassDistributions:
            if self.currentDay >= queryClassDistribution.validFromDay:
                activeQueryClassDistribution = queryClassDistribution

        if oldActiveQueryClassDistribution <> activeQueryClassDistribution:
            self.activeQueryClassDistributionChanged = True

        if activeQueryClassDistribution <> None:
            return activeQueryClassDistribution
        else:
            print "Wrong configuration of query class distributions"

    def determineQueryOrder(self):
        if self.activeQueryClassDistributionChanged == False:
            return

        self.activeQueryClassDistributionChanged = False

        self.currentQueryBatchOrder = map(lambda x: x / DISTRIBUTION_DIVISOR, self.currentlyActiveQueryDistribution.distribution)
        self.currentQueryOrder = []

        for queryBatch in range(0, self.ticksPerDay):
            for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                self.currentQueryOrder.extend([queryClass] * numberOfQueries)


    def run(self):
        while self.currentDay <= self.days:
            self.currentlyActiveQueryDistribution = self.determineCurrentlyActiveDistribution()
            self.determineQueryOrder()

            print "########## Day %i ##########" % (self.currentDay)
            if self.verbose:
                print "Sending %i queries in %i ticks per day à %i queries" % (self.queriesPerDay, self.ticksPerDay, QUERY_DIVISOR / DISTRIBUTION_DIVISOR)
                for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                    print "%i queries of type %s" % (numberOfQueries, queryClass.description)

            for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                self.statistics[queryClass.description].append(numberOfQueries)

            self.threadPoolResults = []

            currentDayQueries = []
            for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                currentDayQueries.extend([(self.ticksPerDay, queryClass.queryFilePath)] * numberOfQueries)

            print currentDayQueries


            self.threadPool.map(simulateDay, currentDayQueries, 1)
            # for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                # self.threadPoolResults.append(self.threadPool.apply_async(queryClass.execute, [self.batches, numberOfQueries], callback = queryClass.addStatistic))


            for threadPoolResult in self.threadPoolResults:
                threadPoolResult.wait()

            self.currentDay += 1

        performanceStatistics = {}
        for queryClass in self.queryClasses:
            performanceStatistics[queryClass.description] = queryClass.statistics
        print "Workload performance: %s" % (performanceStatistics)

        print "Workload statistics: %s" % (self.statistics)

if len(sys.argv) <> 3:
    print "Usage: python generator.py workload.json tableDirectory"
    sys.exit()
else:
    workloadConfigFile = sys.argv[1]
    tableDirectory = sys.argv[2]

    with open(workloadConfigFile) as workloadFile:
            workloadConfig = json.load(workloadFile)

    w = Workload(workloadConfig['days'], workloadConfig['secondsPerDay'], workloadConfig['queryClasses'], workloadConfig['queryClassDistributions'], workloadConfig['verbose'], workloadConfig['compressed'], tableDirectory)
    w.run()