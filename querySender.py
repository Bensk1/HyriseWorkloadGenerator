import config
import numpy as np
import requests
import time
import util

from multiprocessing import Pool
from query import QueryType

THREAD_COUNT = 20
TICK_MS = 0.05

STATISTICAL_FUNCTIONS = {
    'mean': np.mean,
    'median': np.median,
    'percentile90': lambda x: np.percentile(x, 75),
    'total': lambda x: float(reduce(lambda y, z: y + z, x)) / 1000.0
}

def tickSeconds(query):
    nextTick = time.time() + TICK_MS
    r = requests.post("http://localhost:5000/jsonQuery", data=query.text)

    performanceData = r.json()["performanceData"]
    
    try:
        time.sleep(nextTick - time.time())
    except IOError:
        time.sleep(TICK_MS)
        # print "Timer exception"

    return performanceData[-1]["endTime"] - performanceData[0]["startTime"]

def tickCycles(query):
    nextTick = time.time() + TICK_MS
    r = requests.post("http://localhost:5000/jsonQuery", data=query.text)
    performanceData = r.json()["performanceData"]

    try:
        time.sleep(nextTick - time.time())
    except IOError:
        time.sleep(TICK_MS)
        # print "Timer exception"

    cycles = 0
    for performance in performanceData:
        if performance["name"] == "IndexAwareColumnScan":
            cycles += performance["duration"]

    return cycles

class QuerySender:

    def __init__(self):
        self.threadPool = Pool(THREAD_COUNT)
        self.dayTimes = []
        self.statistics = self.initializeStatistics()

        if config.config["statisticsInCycles"]:
            self.tickMethod = tickCycles
        else:
            self.tickMethod = tickSeconds

    def calculateStatistics(self, dailyResults, queries):
        queryStatistics = [[] for i in range(len(QueryType))]
        total = reduce(lambda x,y: x + y, dailyResults)

        for result, query in zip(dailyResults, queries):
            queryStatistics[query.queryType.value].append(result)

        for key in STATISTICAL_FUNCTIONS:
            for qt in QueryType:
                self.statistics[qt.value][key].append(STATISTICAL_FUNCTIONS[key](queryStatistics[qt.value]))

        return total

    def initializeStatistics(self):
        statistics = [{} for i in range(len(QueryType))]

        for key in STATISTICAL_FUNCTIONS:
            for qt in QueryType:
                statistics[qt.value][key] = []

        return statistics

    def sendQueries(self, queries):
        dayBegin = currentTime()
        dailyResults = self.threadPool.map(self.tickMethod, queries, int(len(queries) / THREAD_COUNT) + 1)
        dayEnd = currentTime()

        totalTimeToday = self.calculateStatistics(dailyResults, queries)
        self.dayTimes.append(totalTimeToday)

        print "Total time today: %i - day length: %.2f" % (totalTimeToday, (dayEnd - dayBegin) / 1000.0)
