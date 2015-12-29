class Table:

    def __init__(self, name):
        self.name = name

        # Three queries small (3 attributes), medium (5 attributes), and analytical/explorative (9 attributes)
        self.queries = []
        self.randomQueries = []

        self.generateQueries()

    def generateQueries(self):
        pass