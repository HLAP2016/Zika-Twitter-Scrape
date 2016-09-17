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
    "accept-charset": "utf-8"
  }
}

twitter = Twython(APP_KEY, APP_SECRET, oauth_version=2)
ACCESS_TOKEN = twitter.obtain_access_token()
#print (ACCESS_TOKEN)

twitter = Twython(APP_KEY, access_token=ACCESS_TOKEN)

conn = pymssql.connect(server=r'WIN7ENT-28K87U7\SCIENCE', user='zica', password='zica123!', database='zikatrace')
conn1 = pymssql.connect(server=r'WIN7ENT-28K87U7\SCIENCE', user='zica', password='zica123!', database='zikatrace')

cur = conn.cursor()
cur1 = conn1.cursor()

SQLCommand = ("SELECT userID, max(TwID) TwID from [dbo].[GEO_Twitter_Scrape] where geo <> 'none' group by userID")

cur.execute(SQLCommand)

results = cur.fetchone()

while results:
  tweetID = results[1]
  userID = results[0]
 
  try:
    user_timeline = twitter.get_user_timeline(user_id=userID,count=400,include_retweets=True,since_id=tweetID) #,since_id=tweetID)

    for tweet in user_timeline:
      created_dt = (str((tweet['created_at']))[4:11]+ "20" + str((tweet['created_at']))[-2:])
      #print (tweet['id_str'])
      #print (tweet['user']['id'])
      #print (tweet['user']['screen_name'])
      #print (filter_non_printable(tweet['text'].translate(non_bmp_map)))
      #print (created_dt)
      if tweet["geo"]:
          print(tweet["geo"])
      else:
          print ('None')

      insert_str='INSERT INTO geo_twitter_scrape(twid,userID,cont,username,createdt,geo,crawlingDT) values('+ str(tweet['id_str']) +','+str(tweet['user']['id'])+ ",'"+filter_non_printable(tweet['text'].translate(non_bmp_map)).replace("'"," ")+ "',"  +"'"+ tweet['user']['screen_name'].translate(non_bmp_map) + "',"+"'"+ created_dt + "',"+"'"+ str(tweet['geo']).replace("'"," ") + "',getdate()" + ")"
      print (insert_str)

      cur1.execute(insert_str)
      conn1.commit()

  except:
    print ("exception error")

  results = cur.fetchone()

conn.close()
conn1.close()
