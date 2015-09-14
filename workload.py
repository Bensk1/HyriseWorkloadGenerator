# -*- coding: utf-8 -*-

import sys
import json
import types
import copy_reg
import requests
import glob
import time
from multiprocessing import Pool
from queryClass import QueryClass
from queryClassDistribution import QueryClassDistribution

TICK_MS = 0.2
QUERIES_PER_TICK = 20
DISTRIBUTION_DIVISOR = 100 / QUERIES_PER_TICK
PERIODIC_QUERIES_PER_TICK = 4
RANDOM_QUERIES_PER_TICK = 0
TOTAL_QUERIES_PER_TICK = QUERIES_PER_TICK + PERIODIC_QUERIES_PER_TICK + RANDOM_QUERIES_PER_TICK

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, _pickle_method)

def tick(query):
    if query == None:
        return 0

    r = requests.post("http://localhost:5000/jsonQuery", data=query)
    performanceData = r.json()["performanceData"]

    return performanceData[-1]["endTime"] - performanceData[0]["startTime"]

class Object:
    def toJSON(self):
        return json.dumps(self, default = lambda obj: obj.__dict__, indent = 2)

class Workload(Object):

    def __init__(self, days, secondsPerDay, queryClasses, queryClassDistributions, periodicQueryClasses, verbose, compressed, overallStatistics, indexOptimization, tableDirectory):
        self.days = days
        self.currentDay = 1
        self.secondsPerDay = secondsPerDay
        self.overallStatistics = overallStatistics
        self.tableDirectory = tableDirectory
        self.queryClasses = self.parseQueryClasses(queryClasses, compressed, self.tableDirectory)
        self.queryClassDistributions = self.parseQueryClassDistributions(queryClassDistributions)
        self.periodicQueryClasses = self.parseQueryClasses(periodicQueryClasses, compressed, self.tableDirectory)
        self.verbose = verbose
        self.currentlyActiveQueryDistribution = None
        self.currentQueryOrder = None
        self.currentQueryBatchOrder = None
        self.ticksPerDay = int(1 / TICK_MS * self.secondsPerDay)
        self.indexOptimization = indexOptimization

        self.clearIndexOptimizer()

        self.loadAllTables()

        self.threadPool = Pool(TOTAL_QUERIES_PER_TICK)

        self.statistics = self.initializeStatistics()

    def initializeStatistics(self):
        statistics = {}
        for queryClass in self.queryClasses:
            statistics[queryClass.description] = []
        for periodicQueryClass in self.periodicQueryClasses:
            statistics[periodicQueryClass.description] = []

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
        requests.post("http://localhost:5000/jsonQuery", data = loadTableRequest)

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
        periods = []

        for queryClass in queryClasses:
            queryClassesParsed.append(QueryClass(queryClass['description'], queryClass['table'], queryClass['columns'], queryClass['predicateTypes'], queryClass['compoundExpressions'], queryClass['values'], compressed, tableDirectory, self.days))
            if 'period' in queryClass:
                queryClassesParsed[-1].period = queryClass['period']
                periods.append(queryClass['period'])

        return queryClassesParsed

    def parseQueryClassDistributions(self, queryClassDistributions):
        queryClassDistributionsParsed = []

        for queryClassDistribution in queryClassDistributions:
            queryClassDistributionsParsed.append(QueryClassDistribution(queryClassDistribution['description'], queryClassDistribution['validFromDay'], queryClassDistribution['distribution']))

        return queryClassDistributionsParsed

    def determineCurrentlyActiveDistribution(self):
        activeQueryClassDistribution = None

        for queryClassDistribution in self.queryClassDistributions:
            if self.currentDay >= queryClassDistribution.validFromDay:
                activeQueryClassDistribution = queryClassDistribution

        if activeQueryClassDistribution <> None:
            return activeQueryClassDistribution
        else:
            print "Wrong configuration of query class distributions"

    def determineQueryOrder(self):
        self.currentQueryBatchOrder = map(lambda x: x / DISTRIBUTION_DIVISOR, self.currentlyActiveQueryDistribution.distribution)
        self.currentQueryOrder = []

        for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
            self.currentQueryOrder.extend([(self.ticksPerDay, queryClass)] * numberOfQueries)

    def addPeriodicQueries(self, currentQueryOrder):
        executingPeriodicQueriesToday = False

        for periodicQueryClass in self.periodicQueryClasses:
            if self.currentDay % periodicQueryClass.period == 0:
                executingPeriodicQueriesToday = True
                periodicQueryClass.activeToday = True

                currentQueryOrder.extend([(self.ticksPerDay, periodicQueryClass)] * PERIODIC_QUERIES_PER_TICK)
                self.statistics[periodicQueryClass.description].append(PERIODIC_QUERIES_PER_TICK)
            else:
                periodicQueryClass.activeToday = False

        if not executingPeriodicQueriesToday:
            for periodicQueryClass in self.periodicQueryClasses:
                # Nothing to do today
                currentQueryOrder.extend([(self.ticksPerDay, None)] * PERIODIC_QUERIES_PER_TICK)
                self.statistics[periodicQueryClass.description].append(0)

    def prepareQueries(self):
        self.queries = []
        for currentQuery in self.currentQueryOrder:
            if currentQuery[1] == None:
                self.queries.append(None)
                continue

            queryFile = open(currentQuery[1].queryFilePath, "r")
            query = {}
            query['query'] = queryFile.read()
            query['performance'] = "true"

            queryFile.close()
            self.queries.append(query)

    def simulateDay(self):
        ticksPerDay = self.ticksPerDay
        dayStatistics = [[] for i in range(TOTAL_QUERIES_PER_TICK)]
        resultObjects = []
        statisticsBuffer = []

        nextTick = time.time()
        while ticksPerDay > 0:
            resultObjects.append(self.threadPool.map_async(tick, self.queries, 1))

            ticksPerDay -= 1
            nextTick += TICK_MS

            # wait including drift correction
            time.sleep(nextTick - time.time())

        for resultObject in resultObjects:
            statisticsBuffer.append(resultObject.get())

        for tickStatistics in statisticsBuffer:
            for tickStatistic, queryStatistic in zip(tickStatistics, dayStatistics):
                queryStatistic.append(tickStatistic)

        return dayStatistics

    def calculateOverallStatistics(self, performanceStatistics):
        overall = {}

        for performanceStatistic in performanceStatistics.itervalues():
            for key in performanceStatistic.keys():
                if key not in overall:
                    overall[key] = [0.0 for i in range(len(performanceStatistic[key]))]
                overall[key] =  map(lambda x, y: x + y, overall[key], performanceStatistic[key])

        performanceStatistics['Overall'] = overall

    def clearIndexOptimizer(self):
        clearIndexOptimizerRequest = self.buildClearIndexOptimizerRequest()
        requests.post("http://localhost:5000/jsonQuery", data = clearIndexOptimizerRequest)

        print "Cleared the SelfTunedIndexSelector and dropped all Indexes"

    def buildClearIndexOptimizerRequest(self):
        clearIndexOptimizerRequest = {'query': '{\
            "operators": {\
                "optimizeIndex": {\
                    "type" : "SelfTunedIndexSelection",\
                    "clear": true\
                },\
                "NoOp": {\
                    "type" : "NoOp"\
                }\
            },\
            "edges" : [\
                ["optimizeIndex", "NoOp"]\
            ]\
        }'}

        return clearIndexOptimizerRequest

    def buildIndexOptimizationRequest(self):
        indexOptimizationRequest = {'query': '{\
            "operators": {\
                "optimizeIndex": {\
                    "type" : "SelfTunedIndexSelection"\
                },\
                "NoOp": {\
                    "type" : "NoOp"\
                }\
            },\
            "edges" : [\
                ["optimizeIndex", "NoOp"]\
            ]\
        }'}

        return indexOptimizationRequest

    def triggerIndexOptimization(self):
        indexOptimizationRequest = self.buildIndexOptimizationRequest()
        requests.post("http://localhost:5000/jsonQuery", data = indexOptimizationRequest)

    def run(self):
        while self.currentDay <= self.days:
            self.currentlyActiveQueryDistribution = self.determineCurrentlyActiveDistribution()
            self.determineQueryOrder()

            for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                self.statistics[queryClass.description].append(numberOfQueries)

            self.addPeriodicQueries(self.currentQueryOrder)

            queriesToday = reduce(lambda x, y: (x[0] + y[0], None) if y[1] != None else (x[0], None), self.currentQueryOrder)[0]

            self.prepareQueries()

            dayStatistics = self.simulateDay()

            for query, statistic in zip(self.currentQueryOrder, dayStatistics):
                queryClass = query[1]
                if queryClass <> None:
                    queryClass.addStatistics(self.currentDay, statistic)

            print "########## Day %i ##########" % (self.currentDay)
            if self.verbose:
                print "Sending %i queries in %i ticks per day à %i queries" % (queriesToday, self.ticksPerDay, queriesToday / self.ticksPerDay)
                for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                    print "%i queries of type %s" % (numberOfQueries, queryClass.description)

                for periodicQueryClass in self.periodicQueryClasses:
                    if periodicQueryClass.activeToday:
                        print "%i queries of type %s" % (PERIODIC_QUERIES_PER_TICK, periodicQueryClass.description)
                    else:
                        print "%i queries of type %s" % (0, periodicQueryClass.description)

            if self.indexOptimization:
                self.triggerIndexOptimization()

            self.currentDay += 1

        print
        print "All queries sent. Calculating statistics now..."
        print

        performanceStatistics = {}
        for queryClass in self.queryClasses:
            performanceStatistics[queryClass.description] = queryClass.calculateStatistics()

        for periodicQueryClass in self.periodicQueryClasses:
            performanceStatistics[periodicQueryClass.description] = periodicQueryClass.calculateStatistics()

        if self.overallStatistics:
            self.calculateOverallStatistics(performanceStatistics)

        if 'outputFile' in globals():
            with open(outputFile, 'w') as oFile:
                oFile.write("workloadPerformances = %s\n" % (performanceStatistics))
                oFile.write("workloadStatistics = %s\n" % (self.statistics))
                oFile.close()
                print "Statistics written to %s" % (outputFile)
        else:
            print "workloadPerformances = %s" % (performanceStatistics)
            print "workloadStatistics = %s" % (self.statistics)

if len(sys.argv) < 3:
    print "Usage: python generator.py workload.json tableDirectory [outputFile]"
    sys.exit()
else:
    workloadConfigFile = sys.argv[1]
    tableDirectory = sys.argv[2]
    if len(sys.argv) > 3:
        outputFile = sys.argv[3]

    with open(workloadConfigFile) as workloadFile:
            workloadConfig = json.load(workloadFile)

    w = Workload(workloadConfig['days'], workloadConfig['secondsPerDay'], workloadConfig['queryClasses'], workloadConfig['queryClassDistributions'], workloadConfig['periodicQueryClasses'], workloadConfig['verbose'], workloadConfig['compressed'], workloadConfig['calculateOverallStatistics'], workloadConfig['indexOptimization'], tableDirectory)
    w.run()