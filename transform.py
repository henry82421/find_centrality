import FileTools
import pyes
import json
import FormatTranslator
from sklearn.metrics import pairwise_distances
from scipy.spatial.distance import cosine
import numpy
import csv
import math
import codecs

conn=pyes.es.ES('localhost:9200')
q = pyes.MatchAllQuery()

tagg = pyes.aggs.TermsAgg('name', field= 'user.screen_name', sub_aggs=[]) 
tagg1 = pyes.aggs.TermsAgg('hashtags', field= 'entities.hashtags.text')  
tagg.sub_aggs.append(tagg1) 
qsearch = pyes.query.Search(q) 
qsearch.agg.add(tagg)

rs = conn.search(query=qsearch , indices='ih' ,type="type" )


formatTranslator = FormatTranslator.FormatTranslator()
result = formatTranslator.ES_Aggs_2_Layer_to_Matrix_and_indice(rs.aggs, agg1_name="name", agg2_name="hashtags")

reader = csv.reader(open('C:\matrix.csv', 'rb'))
result=numpy.array(list(csv.reader(open("C:\matrix.csv","rt"),delimiter=','))).astype('float')

for row, item in enumerate(result):
    for col in item:
        if(result.item(row,col)!=0):
            result[row,col]=1
        
print result

from sklearn.feature_extraction.text import TfidfTransformer
tfidf = TfidfTransformer(norm="l2")
tfidf.fit(result)
tf_idf_matrix = tfidf.transform(result)


dist_out = 1-pairwise_distances(tf_idf_matrix.todense(), metric="cosine")


fileTools = FileTools.FileTools()
fileTools.Matrix_to_CSV(dist_out, "x.csv")
