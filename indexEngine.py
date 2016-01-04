import requests

class IndexEngine():

    def __init__(self):
        self.clearIndexOptimizer()

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

    def buildConsolidateIndexOptimizerRequest(self):
        consolidateIndexOptimizerRequest = {'query': '{\
            "operators": {\
                "consolidateSelfTunedIndexSelector": {\
                    "type" : "SelfTunedIndexSelection",\
                    "startNextDayOnly": true\
                },\
                "NoOp": {\
                    "type" : "NoOp"\
                }\
            },\
            "edges" : [\
                ["NoOp", "consolidateSelfTunedIndexSelector"]\
            ]\
        }'}

        return consolidateIndexOptimizerRequest

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
        }', 'performance': 'true'}

        return indexOptimizationRequest

    def clearIndexOptimizer(self):
        clearIndexOptimizerRequest = self.buildClearIndexOptimizerRequest()
        requests.post("http://localhost:5000/jsonQuery", data = clearIndexOptimizerRequest)

        print "Cleared the SelfTunedIndexSelector and dropped all Indexes"

    def triggerConsolidation(self):
        consolidateIndexOptimizerRequest = self.buildConsolidateIndexOptimizerRequest()
        requests.post("http://localhost:5000/jsonQuery", data = consolidateIndexOptimizerRequest)

        print "Consolidated the SelfTunedIndexSelector"

    def triggerIndexOptimization(self):
        indexOptimizationRequest = self.buildIndexOptimizationRequest()
        r = requests.post("http://localhost:5000/jsonQuery", data = indexOptimizationRequest)
        try:
            performanceData = r.json()["performanceData"]
            print "Index Optimization Time: %f" % (performanceData[-1]["endTime"] - performanceData[0]["startTime"])
        except:
            print r.text