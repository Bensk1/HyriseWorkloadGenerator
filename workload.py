# -*- coding: utf-8 -*-

import sys
import json
from queryClass import QueryClass
from queryClassDistribution import QueryClassDistribution

QUERY_DIVISOR = 100
DISTRIBUTION_DIVISOR = 5

class Object:
    def toJSON(self):
        return json.dumps(self, default = lambda obj: obj.__dict__, indent = 2)

class Workload(Object):

    def __init__(self, days, queriesPerDay, queryClasses, queryClassDistributions, verbose):
        self.days = days
        self.currentDay = 1
        self.queriesPerDay = queriesPerDay
        self.queryClasses = self.parseQueryClasses(queryClasses)
        self.queryClassDistributions = self.parseQueryClassDistributions(queryClassDistributions)
        self.verbose = verbose
        self.currentlyActiveQueryDistribution = None
        self.currentQueryOrder = None
        self.activeQueryClassDistributionChanged = False
        self.currentQueryBatchOrder = None
        self.batches = self.queriesPerDay / (QUERY_DIVISOR / DISTRIBUTION_DIVISOR)

    def parseQueryClasses(self, queryClasses):
        queryClassesParsed = []

        for queryClass in queryClasses:
            queryClassesParsed.append(QueryClass(queryClass['description']))

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

        for queryBatch in range(0, self.batches):
            for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                self.currentQueryOrder.extend([queryClass] * numberOfQueries)


    def run(self):
        while self.currentDay <= self.days:
            self.currentlyActiveQueryDistribution = self.determineCurrentlyActiveDistribution()
            self.determineQueryOrder()

            print "########## Day %i ##########" % (self.currentDay)
            if self.verbose:
                print "Sending %i queries in %i batches à %i queries" % (self.queriesPerDay, self.batches, QUERY_DIVISOR / DISTRIBUTION_DIVISOR)
                for numberOfQueries, queryClass in zip(self.currentQueryBatchOrder, self.queryClasses):
                    print "%i queries of type %s" % (numberOfQueries, queryClass.description)

            for query in self.currentQueryOrder:
                query.execute()


            self.currentDay += 1

if len(sys.argv) <> 2:
    print "Usage: python generator.py workload.json"
    sys.exit()
else:
    with open(sys.argv[1]) as workloadFile:
            workloadConfig = json.load(workloadFile)

    w = Workload(workloadConfig['days'], workloadConfig['queriesPerDay'], workloadConfig['queryClasses'], workloadConfig['queryClassDistributions'], workloadConfig['verbose'])
    w.run()