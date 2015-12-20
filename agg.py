import pyes
import json
import codecs
# 單層集合（做兩個不同aggregation）
conn = pyes.es.ES('http://localhost:9200')


q = pyes.query.MatchAllQuery()

file_tianya= codecs.open('D://ag.json', 'w', 'utf8')



tagg = pyes.aggs.TermsAgg('name', field= 'user.screen_name') 
tagg1 = pyes.aggs.TermsAgg('hashtags', field= 'entities.hashtags.text') 
 


qsearch = pyes.query.Search(q) 
qsearch.agg.add(tagg) 
qsearch.agg.add(tagg1)

rs = conn.search(query=qsearch,index='ih',type="type") 


file_tianya.write('{"data": ['+json.dumps(rs.aggs,indent=2)+"]}")
file_tianya.flush()
 



