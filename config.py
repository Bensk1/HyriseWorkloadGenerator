import json

configFile = open("config.json", "r")
config = json.load(configFile)
configFile.close()