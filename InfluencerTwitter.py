################ get related twitter information ####################
import tweepy
import sys, os, csv
import datetime

from klout import *
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

lag30=datetime.date.today()-datetime.timedelta(days=30)
since=lag30.strftime("%Y-%m-%d")

# need to be filled with actual keys from registered twitter app
CONSUMER_KEY = 'your key'
CONSUMER_SECRET = 'your secret'
OAUTH_TOKEN = 'your token'
OAUTH_TOKEN_SECRET = 'your token secret'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.secure = True
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
tweepyApi = tweepy.API(auth, parser=tweepy.parsers.JSONParser(), wait_on_rate_limit=True, wait_on_rate_limit_notify = True)

timeline = tweepyApi.user_timeline(screen_name='levihummon', count = 100, include_rts = True)
timeline = pd.DataFrame.from_dict(timeline)

user = tweepyApi.get_user(screen_name='levihummon')
user = pd.DataFrame.from_dict(user)

followerids = tweepyApi.followers_ids(screen_name='levihummon').get('ids') ##all followers id
followers = tweepyApi.followers(screen_name='levihummon').get('users')
followers = pd.DataFrame.from_dict(followers)

friendids = tweepyApi.friends_ids(screen_name='levihummon').get('ids')  ##all friends ids
#retweeters = tweepyApi.retweeters(id='963878340137451520')


mentionlist = tweepyApi.search('@levihummon',count=100).get('statuses')
mentiondf = pd.DataFrame(mentionlist)
mentionid= pd.DataFrame.to_dict(mentiondf[['user']])
mentionid=pd.DataFrame(mentionid.get('user')).loc['id',:]  ##last 100 mention ids

favoritedf = pd.DataFrame(tweepyApi.favorites(screen_name='levihummon',count=200))
favoriteid= pd.DataFrame.to_dict(favoritedf[['user']])
favoriteid=pd.DataFrame(favoriteid.get('user')).loc['id',:]  ##last 100 favorite ids

followid=pd.DataFrame(followerids,columns=['id'])
followid['type']='follower'
friendid=pd.DataFrame(friendids,columns=['id'])
friendid['type']='friend'
mentionid=pd.DataFrame(mentionid,columns=['id'])
mentionid['type']='mention'
favoriteid=pd.DataFrame(favoriteid,columns=['id'])
favoriteid['type']='favorite'
iddf=followid.append(friendid).append(mentionid).append(favoriteid)

uniqueiddf = iddf.groupby(['id','type']).agg({'id':['first','count'],'type': "first"})
uniqueiddf.columns=['id','freq','type']
uniqueiddf=pd.pivot_table(uniqueiddf, index=['id'],columns=['type'],values='freq')
uniqueiddf['id']=list(uniqueiddf.index)
#uniqueiddf['type'][uniqueiddf['freq']>1]='active'

##################  append klout ######################
##################  get Klout score ###################
## Ooooooooops!!!!! https://www.lithium.com/products/klout
## Klout is now kaput!!!
## https://klout.com/corp/score
## https://arxiv.org/pdf/1510.08487.pdf
## explainations to klout score, pretty good algorithm

# Make the Klout object
#k = Klout('snhep67ynnzb79mztavd5ewm')

## Get kloutId of the user by inputting a twitter screenName
kloutId = k.identity.klout(screenName="the screenname you are interested in").get('id')
## Get klout score of the user
score = k.user.score(kloutId=kloutId).get('score')

#influence = k.user.influence(kloutId=kloutId)  ##myinluencers and myinfluenees
#topic = k.user.topics(kloutId=kloutId) ##5 related topics
#pd.DataFrame(topic).name
#print ("User's klout score is: %s" % (score))

# By default all communication is not secure (HTTP). An optional secure parameter
# can be sepcified for secure (HTTPS) communication
#k = Klout('snhep67ynnzb79mztavd5ewm', secure=True)

# Optionally a timeout parameter (seconds) can also be sent with all calls
#score = k.user.score(kloutId=kloutId, timeout=5).get('score')

user = [0]*len(uniqueiddf.id)
#user = [0]*5
#for i in range(5):
for i in range(len(uniqueiddf.id)):
    user[i]=pd.DataFrame(tweepyApi.get_user(user_id=list(uniqueiddf.id)[i])).iloc[0,:]
    user[i]=pd.DataFrame(user[i]).transpose()

df = pd.concat(user)

#############

k = Klout('snhep67ynnzb79mztavd5ewm')

# Get kloutId of the user by inputting a twitter screenName
#counter=0
def try_kloutid(x):
    counter=0
    try: 
        kloutId=k.identity.klout(screenName=x).get('id')
        return kloutId
#        k = Klout('snhep67ynnzb79mztavd5ewm', secure=True)
        # Optionally a timeout parameter (seconds) can also be sent with all calls
        counter = counter+1
#        kloutId = k.identity.klout(screenName=x, timeout=1).get('id')
    except KloutHTTPError as e:
#        print( "Oops! no kloudId found with that name. Try again... "),counter
        return 'Oops!'
        
def try_kloutscore(x):
    counter=0
    try: 
        kloutscore=k.user.score(kloutId=x).get('score')
        return kloutscore
        counter = counter+1
    except KloutHTTPError as e:
        return ''
        
    
def try_klouttopic(x):
    counter=0
    try: 
        klouttopic=' '.join(list(pd.DataFrame(k.user.topics(kloutId=x)).slug))
        return klouttopic
        counter = counter+1
    except (KloutHTTPError,AttributeError) as e:
        return ''
             
                
# Get klout id of the user
df['kloutId'] = list(map(lambda x: try_kloutid(x),list(df.screen_name)))
# Get klout score of the user

##try another key
k = Klout('5whgqjspbc3mjwgwuq3csuat')
df['kloutscore']=''
df['kloutscore'][df['kloutId']!='Oops!'] = list(map(lambda x: try_kloutscore(x),list(df['kloutId'][df['kloutId']!='Oops!'])))

#df['klouttopic']=''
#df['klouttopic'][df['kloutId']!='Oops!']= list(map(lambda x: try_klouttopic(x),list(df['kloutId'][df['kloutId']!='Oops!'])))
df['klouttopic']= list(map(lambda x: try_klouttopic(x),list(df['kloutId'])))


def tokenize(text):
    return(text.split(' '))
vec = CountVectorizer(tokenizer=tokenize)

matrix=vec.fit_transform(list(df['klouttopic'])).toarray()

key_words=vec.get_feature_names()

textdf=pd.DataFrame(matrix,columns=key_words)

uniqueiddf.index=range(len(uniqueiddf['id']))
df.index=range(len(df['id']))
totaldf=pd.concat([uniqueiddf,df,textdf], axis=1)

totaldf.to_csv("~/Influencer/twitter.csv",index=False)
            
tweepyApi.get_user(screen_name='screenname').get('id')        
artistdf=pd.DataFrame(pd.DataFrame(tweepyApi.get_user(user_id='user id')).iloc[0,:]).transpose()
artistdf['kloutscore']=try_kloutscore(try_kloutid('name'))
artistdf=artistdf[['id','name','location','description','kloutscore','followers_count']]
