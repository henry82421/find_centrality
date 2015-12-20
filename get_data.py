import pyes
import json
import codecs

conn = pyes.es.ES('http://localhost:9200')


file_tianya= codecs.open('D://ihprint.json', 'w', 'utf8')

bq = pyes.query.BoolQuery() 
ESR = pyes.ESRange(field="created_at", from_value="Mon June 1 00:00:00 +0000 2015", to_value="Sun June 7 00:00:00 +0000 2015", include_lower=True ,include_upper=False)


bq.add_must(pyes.query.RangeQuery(qrange=ESR))



bf = pyes.filters.BoolFilter()

bf.add_must(pyes.filters.ExistsFilter(field="entities.hashtags.text"))
fq=pyes.query.FilteredQuery(pyes.query.RangeQuery(qrange=ESR), bf)



result = conn.search(query=fq , indices='twitter2' , doc_types='tweet',fields="uid,user.screen_name,entities.hashtags.text")




for data in result:
 file_tianya.write(json.dumps(data,indent=3) + ",\n")

 file_tianya.flush()
 
