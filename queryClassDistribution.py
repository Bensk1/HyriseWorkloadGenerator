PERCENT_MULTIPLIER = 100

class QueryClassDistribution:

    def __init__(self, description, validFromDay, distribution):
        self.description = description
        self.validFromDay = validFromDay
        self.distribution = self.parseDistribution(distribution)

    def parseDistribution(self, distribution):
        return map(lambda x: int(x * PERCENT_MULTIPLIER), distribution)