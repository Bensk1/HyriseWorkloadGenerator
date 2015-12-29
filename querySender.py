import config
import requests
import time
import util

from multiprocessing import Pool

THREAD_COUNT = 20
TICK_MS = 0.05

def tickSeconds(query):
    nextTick = time.time() + TICK_MS
    r = requests.post("http://localhost:5000/jsonQuery", data=query)

    performanceData = r.json()["performanceData"]
    
    try:
        time.sleep(nextTick - time.time())
    except IOError:
        time.sleep(TICK_MS)
        # print "Timer exception"

    return performanceData[-1]["endTime"] - performanceData[0]["startTime"]

def tickCycles(query):
    nextTick = time.time() + TICK_MS
    r = requests.post("http://localhost:5000/jsonQuery", data=query)
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

        if config.config["statisticsInCycles"]:
            self.tickMethod = tickCycles
        else:
            self.tickMethod = tickSeconds

    def sendQueries(self, queries):
        result = self.threadPool.map(self.tickMethod, queries, len(queries) / THREAD_COUNT)