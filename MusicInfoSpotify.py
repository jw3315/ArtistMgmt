import sys
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import pprint
import pandas as pd
import numpy as np
import io
from pandas.io.json import json_normalize
import json
import csv
import matplotlib.pyplot as plt
import time

client_id = 'your spotify api id'
client_secret = 'your spotify api secret'

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
#client_credentials_manager.token_info

###### search songs ##########       

years1995=['1995','1996','1997','1998','1999','2000','2001',
              '2002','2003','2004','2005','2006','2007','2008',
              '2009','2010','2011','2012','2013','2014','2015','2016','2017']
def num(YEAR):
    return sp.search('year:'+YEAR, type='track').get('tracks').get('total')
track_per_year=[num(YEAR) for YEAR in years1995]
plt.plot(years1995,track_per_year)

num_per_search=100000  ##max offset 100,000
OFFSET=0   
YEARS=['2013','2014','2015','2016','2017']

def search_result(YEAR):
    search_result=sp.search('year:'+YEAR, limit=50, offset=OFFSET, type='track', market=None)
    items=search_result.get('tracks').get('items')
    return pd.DataFrame(items)

items=pd.DataFrame()
while OFFSET<num_per_search:
    for YEAR in YEARS:
        items=items.append(search_result(YEAR))
    OFFSET +=50
    time.sleep(0.3) 
items = items.reset_index(drop=True)  
items.to_csv('~/spotify500k_items.csv',index = False)   

album=pd.DataFrame(json_normalize(items['album']))
album=album.rename(columns=lambda x: 'album_'+x)
artists=list(items['artists'])
artist_total=pd.DataFrame()
for artist in artists:
    artistdf=pd.DataFrame(artist)
    index=artists.index(artist)
    artistdf['track_id']=items['id'][index]
    artist_total=artist_total.append(artistdf)
artist_total=artist_total.rename(columns=lambda x: 'artist_'+x)
artist_total=artist_total.drop_duplicates(subset=['artist_id','artist_track_id'],keep='first')
item_detail=pd.concat([items,album],axis=1).drop('album',axis=1)
df1=item_detail.merge(artist_total,left_on='id', right_on='artist_track_id', how='inner')


        
###### get audio features ################
df2=pd.DataFrame()
start=0
while start<df1.shape[0]:
     df2=df2.append(pd.DataFrame(sp.audio_features(df1['id'][start:start+50])))
     start += 50
     time.sleep(0.3)
df2=df2.rename(columns=lambda x: 'audio_'+x)
df1 = df1.reset_index(drop=True)
df2 = df2.reset_index(drop=True)
#df=pd.concat([df1,df2],axis=1)


######### get artist features #####################
df3=pd.DataFrame()
#for id in df['artist_id']:
#    try:
#        genre=sp.artist(id).get('genres')[0]
#    except IndexError:
#        genre='no genres'

start=0
while start< df1.shape[0]:
    artist_feature=pd.DataFrame(sp.artists(df1['artist_id'][start:start+50]).get('artists'))
#    genre=','.join(artists.get('genres'))
#    artist_feature=np.array([artists.get('popularity'),
#                                 artists.get('followers').get('total'),
#                                          genre
#                                          ]).reshape((1,3))
#    artist_feature=pd.DataFrame(artist_feature,columns=['artist_popularity','artist_followers','genre'])
    df3=df3.append(artist_feature)
    start += 50
    time.sleep(0.3)
    
#    genre=','.join(sp.artist(id).get('genres'))
#    artist_feature=np.array([sp.artist(id).get('popularity'),
#                                 sp.artist(id).get('followers').get('total'),
#                                          genre
#                                          ]).reshape((1,3))
#    artist_feature=pd.DataFrame(artist_feature,columns=['artist_popularity','artist_followers','genre'])
#    df3=df3.append(artist_feature)
df3 = df3.reset_index(drop=True)
#df3=df3[['followers', 'genres','popularity']]
df3=pd.concat([df3[['genres','popularity']],pd.DataFrame(list(df3['followers']))['total']],axis=1)
df3=df3.rename(columns={'genres':'genre','popularity':'artist_popularity','total':'artist_followers'})
#               lambda x: 'artist_'+x)

df=pd.concat([df,df3],axis=1)
df.to_csv('~/spotify1m_withcorp.csv',index = False)
########### final df ##############################
df_sub=df[['id','name','popularity','track_number','duration_ms','explicit',
          'album_id','album_name', 'album_release_date', 'album_album_type',
       'artist_id', 'artist_name', 'artist_popularity','artist_followers','genre',
       'audio_id','audio_acousticness',
       'audio_danceability', 'audio_duration_ms', 'audio_energy', 
       'audio_instrumentalness', 'audio_key', 'audio_liveness',
       'audio_loudness', 'audio_mode', 'audio_speechiness', 'audio_tempo',
       'audio_time_signature','audio_valence']]

df_sub=df_sub.sort_values(by=['artist_popularity'],ascending=False)
df_dedup=df_sub.drop_duplicates(subset=['id'],keep='first')
df_dedup=df_dedup.fillna("NA")

#df_dedup.to_csv('~/spotify.csv',index = False)
df_dedup.to_csv('~/spotify1m.csv',index = False)
###################### Levi, Kelsea... ##############################################
# urn = '64fJiKnU2RfnndB8xP3gLi'  ## artist id
artistname='Kelsea Ballerini'
urn = '3RqBeV12Tt7A8xH3zBDDUF'  ## Kelsea Ballerini
#urn = '64fJiKnU2RfnndB8xP3gLi'  ##Levi
artist = sp.artist(urn)
# artist = sp.search('Kelsea Ballerini',type='artist') 
albums = sp.artist_albums(urn)
related = sp.artist_related_artists(urn)
top = sp.artist_top_tracks(urn, country='US')
artist['followers'] = artist['followers']['total']
artist['genres'] = ','.join(artist['genres'])


del artist["external_urls"]
del artist["images"]
artist = pd.DataFrame(artist,index=range(1)) ##id is for artist id
artist


album_key = ['album_type', 'href', 'id',  'name', 'type', 'uri', 'artistid']
album = {}
num_album = len(pd.DataFrame(albums).index)
for i in range(num_album):
    album[i] = pd.DataFrame(albums).iloc[i,1]
    album[i]['artistid'] = album[i]['artists'][0]['id']
    album[i] = dict((k, album[i][k]) for k in album_key)
    album[i] = pd.DataFrame(album[i],index = range(1))

artistalbum = pd.concat([album[i] for i in range(num_album)],ignore_index=True)
artistalbum


num_relate = len(pd.DataFrame(related).index)
relate = {}
relate_key = [ 'followers', 'genres', 'href', 'id', 'name', 'popularity', 'type', 'uri']
for i in range(num_relate):
    relate[i] = pd.DataFrame(related).iloc[i,0]
    relate[i]['followers'] = relate[i]['followers']['total']
    relate[i]['genres'] = ','.join(relate[i]['genres'])
    relate[i] = dict((k, relate[i][k]) for k in relate_key)
    relate[i] = pd.DataFrame(relate[i],index = range(1))
relatedsinger = pd.concat([relate[i] for i in range(num_relate)],ignore_index=True)



pd.DataFrame(top)

num_top = len(pd.DataFrame(top).index)
toptrack = {}
top_key = ['albumid', 'artistid', 'disc_number', 'duration_ms', 'explicit', 'href', 'id', 'name', 'popularity', 'preview_url', 'track_number', 'type', 'uri']
for i in range(num_top):
    toptrack[i] = pd.DataFrame(top).iloc[i,0]
    toptrack[i]['albumid'] = toptrack[i]['album']['id']
    toptrack[i]['artistid'] = toptrack[i]['artists'][0]['id']
    toptrack[i] = dict((k, toptrack[i][k]) for k in top_key)
    toptrack[i] = pd.DataFrame(toptrack[i],index = range(1))
artisttrack = pd.concat([toptrack[i] for i in range(num_top)],ignore_index=True)
# pd.DataFrame(top).iloc[0,0]
# toptrack[i]['artists']



artisttrack


# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(artistname+'spotify.xlsx', engine='xlsxwriter')

# Write each dataframe to a different worksheet.
artist.to_excel(writer, sheet_name='artist',index=False)
artistalbum.to_excel(writer, sheet_name='artistalbum',index=False)
relatedsinger.to_excel(writer, sheet_name='relatedsinger',index=False)
artisttrack.to_excel(writer, sheet_name='artisttrack',index=False)
# recentdfYOUTUBE.to_excel(writer, sheet_name='recentdfYOUTUBE',index=False)
# recentdfVEVO.to_excel(writer, sheet_name='recentdfVEVO',index=False)
# reachdf.to_excel(writer, sheet_name='reach',index=False)
# engagedf.to_excel(writer, sheet_name='engagement',index=False)

# Close the Pandas Excel writer and output the Excel file.
writer.save()
