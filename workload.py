# -*- coding: utf-8 -*-

import sys
import json
import types
import copy_reg
import requests
import glob
import time
import itertools
from multiprocessing import Pool
from queryClass import QueryClass
from queryClassDistribution import QueryClassDistribution

TICK_MS = 0.2
QUERIES_PER_TICK = 20
DISTRIBUTION_DIVISOR = 100 / QUERIES_PER_TICK
PERIODIC_QUERIES_PER_TICK = 4
RANDOM_QUERIES_PER_TICK = 0
TOTAL_QUERIES_PER_TICK = QUERIES_PER_TICK + PERIODIC_QUERIES_PER_TICK + RANDOM_QUERIES_PER_TICK
REPEAT_BEST_INDEX_RUNS = 3

def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

copy_reg.pickle(types.MethodType, _pickle_method)

def tickSeconds(query):
    if query == None:
        return 0

    r = requests.post("http://localhost:5000/jsonQuery", data=query)
    performanceData = r.json()["performanceData"]

    return performanceData[-1]["endTime"] - performanceData[0]["startTime"]

def tickCycles(query):
    if query == None:
        return 0

    r = requests.post("http://localhost:5000/jsonQuery", data=query)
    performanceData = r.json()["performanceData"]

    cycles = 0
    for performance in performanceData:
        if performance["name"] == "IndexAwareColumnScan":
            cycles += performance["duration"]

    return cycles

def returnAndIncrement(i):
    value = i[0]
    i[0] += 1

    return value

class Object:
    def toJSON(self):
        return json.dumps(self, default = lambda obj: obj.__dict__, indent = 2)

class Workload(Object):

    def __init__(self, days, secondsPerDay, queryClasses, queryClassDistributions, periodicQueryClasses, verbose, compressed, overallStatistics, statisticsInCycles, indexOptimization, findBestIndexConfiguration, availableBudget, tableDirectory):
        self.days = days
        self.currentDay = 1
        self.secondsPerDay = secondsPerDay
        self.overallStatistics = overallStatistics
        self.statisticsInCycles = statisticsInCycles
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
        self.availableBudget = availableBudget
        self.findBestIndexConfiguration = findBestIndexConfiguration

        self.clearIndexOptimizer(False)

        self.loadAllTables()

        if self.findBestIndexConfiguration:
            self.indexConfigurations = self.determineIndexConfigurations()

        self.threadPool = Pool(TOTAL_QUERIES_PER_TICK)

        self.statistics = self.initializeStatistics()

    def initializeStatistics(self):
        statistics = {}
        for queryClass in self.queryClasses:
            statistics[queryClass.description] = []
        for periodicQueryClass in self.periodicQueryClasses:
            statistics[periodicQueryClass.description] = []

        return statistics

    def determineIndexConfigurations(self):
        tableColumns = []

        for queryClass in self.queryClasses:
            tableColumn = "%s" % (queryClass.table)

            for column in queryClass.columns:
                tableColumn += "_%s" % (column)

            tableColumns.append(tableColumn)

        configurations = []
        for i in range(len(tableColumns)):
            configurations.append(list(itertools.combinations(tableColumns, i + 1)))

        return self.checkConfigurationsMemoryBudget(configurations)

    def checkConfigurationsMemoryBudget(self, configurations):
        viableConfigurations = []

        for configurationClasses in configurations:
            viableConfigurationsLen = len(viableConfigurations)
            for configuration in configurationClasses:
                availableBudget = self.availableBudget
                for tableColumn in configuration:
                    approximateIndexFootpringRequest = self.buildApproximateIndexFootprintRequest(tableColumn)
                    r = requests.post("http://localhost:5000/jsonQuery", data = approximateIndexFootpringRequest)
                    footprint = r.json()['rows'][0][0]
                    availableBudget -= footprint

                if availableBudget > 0:
                    viableConfigurations.append(configuration)

            # No new configurations from this class? We can skip all following classes because they get even bigger
            if len(viableConfigurations) <= viableConfigurationsLen:
                break

        return viableConfigurations

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

        for queryClass in queryClasses:
            queryClassesParsed.append(QueryClass(queryClass['description'], queryClass['table'], queryClass['columns'], queryClass['predicateTypes'], queryClass['compoundExpressions'], queryClass['values'], compressed, tableDirectory, self.days))
            if 'period' in queryClass:
                queryClassesParsed[-1].period = queryClass['period']
                i = queryClass['start']
                while i < self.days:
                    for day in range(queryClass['duration']):
                        queryClassesParsed[-1].periodDays.append(i + day)

                    i += queryClassesParsed[-1].period

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
            if self.currentDay in periodicQueryClass.periodDays:
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

        if self.statisticsInCycles:
            tickMethod = tickCycles
        else:
            tickMethod = tickSeconds

        nextTick = time.time()
        while ticksPerDay > 0:
            resultObjects.append(self.threadPool.map_async(tickMethod, self.queries, 1))

            ticksPerDay -= 1
            nextTick += TICK_MS

            # wait including drift correction
            try:
                time.sleep(nextTick - time.time())
            except IOError:
                time.sleep(TICK_MS)
                nextTick += TICK_MS
                print "Timer exception"

        for resultObject in resultObjects:
            statisticsBuffer.append(resultObject.get())

        for tickStatistics in statisticsBuffer:
            for tickStatistic, queryStatistic in zip(tickStatistics, dayStatistics):
                queryStatistic.append(tickStatistic)

        return dayStatistics

    def calculateOverallStatistics(self, performanceStatistics):
        overall = {}

        for performanceStatistic, workloadStatistic in zip(performanceStatistics.itervalues(), self.statistics.itervalues()):
            day = [0]
            for key in performanceStatistic.keys():
                if key not in overall:
                    overall[key] = [0.0 for i in range(len(performanceStatistic[key]))]
                if key == "mean":
                    overall[key] =  map(lambda x, y: x + y * workloadStatistic[returnAndIncrement(day)], overall[key], performanceStatistic[key])
                else:
                    overall[key] =  map(lambda x, y: x + y, overall[key], performanceStatistic[key])

        overall["mean"] = map(lambda x: x / QUERIES_PER_TICK,  overall["mean"])
        performanceStatistics['Overall'] = overall

    def clearIndexOptimizer(self, silent):
        clearIndexOptimizerRequest = self.buildClearIndexOptimizerRequest()
        requests.post("http://localhost:5000/jsonQuery", data = clearIndexOptimizerRequest)

        if not silent:
            print "Cleared the SelfTunedIndexSelector and dropped all Indexes"

    def buildApproximateIndexFootprintRequest(self, tableColumn):
        approximateIndexFootprintRequest = {'query': '{\
            "operators": {\
                "approximateIndex": {\
                    "type" : "SelfTunedIndexSelection",\
                    "approximateIndexFootprint": true,\
                    "tableColumn": "%s"\
                },\
                "NoOp": {\
                    "type" : "NoOp"\
                }\
            },\
            "edges" : [\
                ["NoOp", "approximateIndex"]\
            ]\
        }' % (tableColumn)}

        return approximateIndexFootprintRequest

    def buildClearIndexOptimizerRequest(self):
        clearIndexOptimizerRequest = {'query': '{\
            "operators": {\
                "clearSelfTunedIndexSelector": {\
                    "type" : "SelfTunedIndexSelection",\
                    "clear": true\
                },\
                "NoOp": {\
                    "type" : "NoOp"\
                }\
            },\
            "edges" : [\
                ["NoOp", "clearSelfTunedIndexSelector"]\
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

    def buildDropIndexRequest(self, tableColumn):
        table = tableColumn.split('_')[0]

        dropIndexRequest = {'query': '{\
            "operators": {\
                "drop": {\
                    "type": "DropIndex",\
                    "indexName": "findBestIndexConfiguration_%s",\
                    "tableName": "%s"\
                }\
            },\
            "edges" : [\
                ["drop", "drop"]\
            ]\
        }' % (tableColumn, table)}

        return dropIndexRequest

    def buildDropAndCreateIndexRequest(self, tableColumn):
        table = tableColumn.split('_')[0]
        column = tableColumn.split('_')[1]

        dropAndCreateIndexRequest = {'query': '{\
            "operators": {\
                "drop": {\
                    "type": "DropIndex",\
                    "indexName": "findBestIndexConfiguration_%s",\
                    "tableName": "%s"\
                },\
                "get": {\
                    "type" : "GetTable",\
                    "name" : "%s"\
                },\
                "create": {\
                    "type": "CreateGroupkeyIndex",\
                    "fields": [%s],\
                    "index_name": "findBestIndexConfiguration_%s",\
                    "forceCompoundValueIdKey": true\
                }\
            },\
            "edges" : [\
                ["drop", "get"],\
                ["get", "create"]\
            ]\
        }' % (tableColumn, table, table, column, tableColumn)}
        return dropAndCreateIndexRequest

    def triggerIndexOptimization(self):
        indexOptimizationRequest = self.buildIndexOptimizationRequest()
        requests.post("http://localhost:5000/jsonQuery", data = indexOptimizationRequest)

    def runScenario(self):
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

            if self.indexOptimization and not self.findBestIndexConfiguration:
                self.triggerIndexOptimization()

            self.currentDay += 1

    def createIndexesForConfiguration(self, configuration):
        for tableColumn in configuration:
            dropAndCreateIndexRequest = self.buildDropAndCreateIndexRequest(tableColumn)
            r = requests.post("http://localhost:5000/jsonQuery", data = dropAndCreateIndexRequest)
            print "Created Index on %s" % (tableColumn)

    def dropIndexesForConfiguration(self, configuration):
        for tableColumn in configuration:
            dropIndexRequest = self.buildDropIndexRequest(tableColumn)
            r = requests.post("http://localhost:5000/jsonQuery", data = dropIndexRequest)
            print "Dropped Index on %s" % (tableColumn)

    def calculateFinalStatistics(self, performanceStatistics):
        for queryClass in self.queryClasses:
            performanceStatistics[queryClass.description] = queryClass.calculateStatistics()

        for periodicQueryClass in self.periodicQueryClasses:
            performanceStatistics[periodicQueryClass.description] = periodicQueryClass.calculateStatistics()

        if self.overallStatistics:
            self.calculateOverallStatistics(performanceStatistics)

    def checkConfigurationPerformances(self, bestTotalConfiguration, bestBeginningConfiguration, performanceStatistics, indexConfiguration):
        totalTime = reduce(lambda x, y: x + y, performanceStatistics)
        self.checkConfigurationPerformance(totalTime, bestTotalConfiguration, performanceStatistics, indexConfiguration)

        dayOneTime = performanceStatistics[0]
        self.checkConfigurationPerformance(dayOneTime, bestBeginningConfiguration, performanceStatistics, indexConfiguration)

    def checkConfigurationPerformance(self, time, best, performance, indexConfiguration):
        if time < best['time']:
            best['time'] = time
            best['configuration'] = indexConfiguration
            best['total'] = performance
            print best

    def reset(self):
        self.currentDay = 1
        self.clearIndexOptimizer(True)
        self.statistics = self.initializeStatistics()

        for queryClass in self.queryClasses:
            queryClass.reset()

        for periodicQueryClass in self.periodicQueryClasses:
            periodicQueryClass.reset()


    def run(self):
        if self.findBestIndexConfiguration:
            bestTotalConfiguration = {'time': sys.float_info.max, 'configuration': [], 'total': []}
            bestBeginningConfiguration = {'time': sys.float_info.max, 'configuration': [], 'total': []}

            for indexConfiguration in self.indexConfigurations:
                self.createIndexesForConfiguration(indexConfiguration)

                averagePerformanceStatistics = [0 for i in range(self.days)]
                for i in range(REPEAT_BEST_INDEX_RUNS):
                    performanceStatistics = {}
                    self.runScenario()

                    self.calculateFinalStatistics(performanceStatistics)
                    averagePerformanceStatistics = map(lambda x, y: x + y, averagePerformanceStatistics, performanceStatistics['Overall']['total'])

                    self.reset()

                averagePerformanceStatistics = map(lambda x: x / REPEAT_BEST_INDEX_RUNS, averagePerformanceStatistics)
                self.checkConfigurationPerformances(bestTotalConfiguration, bestBeginningConfiguration, averagePerformanceStatistics, indexConfiguration)

                self.dropIndexesForConfiguration(indexConfiguration)

            print "Final best configuration:"
            print bestTotalConfiguration
            print "###"
            print "Final best starting configuration:"
            print bestBeginningConfiguration
        else:
            self.runScenario()

            print
            print "All queries sent. Calculating statistics now..."
            print

            performanceStatistics = {}
            self.calculateFinalStatistics(performanceStatistics)

            if 'outputFile' in globals():
                with open(outputFile, 'w') as oFile:
                    oFile.write("threads = %i\n" % (QUERIES_PER_TICK))
                    oFile.write("secondsPerDay = %f\n" % (self.secondsPerDay))
                    oFile.write("workloadPerformances = %s\n" % (performanceStatistics))
                    oFile.write("workloadStatistics = %s\n" % (self.statistics))
                    oFile.close()
                    print "Statistics written to %s" % (outputFile)
            else:
                print "threads = %i" % (QUERIES_PER_TICK)
                print "secondsPerDay = %f" % (self.secondsPerDay)
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

    w = Workload(workloadConfig['days'], workloadConfig['secondsPerDay'], workloadConfig['queryClasses'], workloadConfig['queryClassDistributions'], workloadConfig['periodicQueryClasses'], workloadConfig['verbose'], workloadConfig['compressed'], workloadConfig['calculateOverallStatistics'], workloadConfig['statisticsInCycles'], workloadConfig['indexOptimization'], workloadConfig['findBestIndexConfiguration'], workloadConfig['availableBudget'], tableDirectory)
    w.run()