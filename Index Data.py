import pyes
import json


conn = pyes.es.ES('http://localhost:9200')






dataset_json = open("D://ihprint.json")
dataset = json.load(dataset_json)['data']
for data in dataset:
    conn.index(data, "ooooo", "type", "id_"+str(dataset.index(data)))