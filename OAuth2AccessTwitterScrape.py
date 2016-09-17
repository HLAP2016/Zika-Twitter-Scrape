from twython import Twython
import sys

import pymssql

def filter_non_printable(str):
  return ''.join([c for c in str if ord(c) > 31 or ord(c) == 9])

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

APP_KEY = 'YMOWp2O8ouHgZGc1MurZOTTPu'
APP_SECRET = 'MalkvtRCkWpNEcurcZ64332NmPktDbfg9D4p6Mj40szQL5rkOb'

client_args = {
  "headers": {
    "accept-charset": "utf-8"}
}

twitter = Twython(APP_KEY, APP_SECRET, oauth_version=2)
ACCESS_TOKEN = twitter.obtain_access_token()
#print (ACCESS_TOKEN)

twitter = Twython(APP_KEY, access_token=ACCESS_TOKEN)
  

#geocode = '-23.022236,-43.077393,500mi'# latitude,longitude,distance(mi/km) for Rio de Janerio
#geocode = '40.644699,-73.883057,100mi'#New York City
#geocode = '25.761681,-80.191788,100mi' #Miami

geocode_list=['-23.022236,-43.077393,500mi','40.644699,-73.883057,100mi','25.761681,-80.191788,100mi']
city_list=['Rio de Janeiro', 'New York City', 'Miami']
keyword_list = ['pregnant','grávida','eye pain','dor nos olhos','vomit','vomitar','muscle pain','dor muscular','mosquito','Zika','feel sick','doente','fever','febre','rash','erupção','erupção cutânea','joint pain','dor nas articulações','doctor','médico','doctor appointment','consulta médica','mosquito bite','picada de mosquito','bug repellent','bug repellant','repelente de insetos','mosquito repellant','mosqutio repellent','repelente de mosquito','bug spray']

for i in range(0,3):
  geocode = geocode_list[i]
  city = city_list[i]

  for keyword in keyword_list:
      search_results = twitter.search(count=5,q=keyword,geocode=geocode,since= '2016-09-08',until='2016-09-09')

      conn = pymssql.connect(server=r'WIN7ENT-28K87U7\SCIENCE', user='zica', password='zica123!', database='zikatrace')

      cur = conn.cursor()

      for tweet in search_results['statuses']:

          created_dt = (str((tweet['created_at']))[4:11]+ "20" + str((tweet['created_at']))[-2:])
          print (tweet['id_str'])
          print (tweet['user']['id'])
          print (filter_non_printable(tweet['text'].translate(non_bmp_map)))
          print (tweet['user']['geo_enabled'])
          print (tweet['user']['screen_name'])
          print (created_dt)
          if tweet["geo"]:
              print(tweet["geo"])
          else:
              print ('None')
          

          insert_str='INSERT INTO twitter_scrape(twid,userID,searchKey,geoenable,cont,username,createdt,geo,crawlingDT,searchgeo,searchcity) values('+ str(tweet['id_str']) +','+str(tweet['user']['id'])+ ",'"+keyword +"','"+str(tweet['user']['geo_enabled']) + "','" +filter_non_printable(tweet['text'].translate(non_bmp_map)).replace("'"," ")+ "',"  +"'"+ tweet['user']['screen_name'].translate(non_bmp_map) + "',"+"'"+ created_dt + "',"+"'"+ str(tweet['geo']).replace("'"," ") + "',getdate()" + ",'" + geocode + "','" + city + "')"
          print(insert_str)
          cur.execute(insert_str)
          conn.commit()


conn.close()
        


