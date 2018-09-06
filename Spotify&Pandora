## pull Spotify and Pandora stream data 

######pull spotify api data to make it easier ##########
import sys
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import pprint
import pandas as pd
import numpy as np
import csv

## api credentials
client_id = 'your api id'
client_secret = 'your api secret'

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

artistname='your artist name'
urn = 'your artist urn'
artist = sp.artist(urn)
# artist = sp.search(artistname,type='artist')
albums = sp.artist_albums(urn)
related = sp.artist_related_artists(urn)
top = sp.artist_top_tracks(urn, country='US')
artist['followers'] = artist['followers']['total']
artist['genres'] = ','.join(artist['genres'])

## artist information
del artist["external_urls"]
del artist["images"]
artist = pd.DataFrame(artist,index=range(1)) ##id is for artist id
artist

## artist album information
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

## related singers information
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


## track information
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

artisttrack.id

##################### pull data from spotify for artist ############################
## as a artist manager, you must have registered and used spotify for artist to review the performance of your artist.
## import modules
from selenium import webdriver
import time
import datetime
from time import gmtime, strftime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import requests
from bs4 import BeautifulSoup

import re
import numpy as np
import pandas as pd

import json
from functools import reduce
from selenium.webdriver.support.ui import Select

## login to spotify for artist
## download chromedriver from http://chromedriver.chromium.org/downloads
browser = webdriver.Chrome('~/chromedriver_win32/chromedriver.exe')
browser.get("https://accounts.spotify.com/en/login?continue=https:%2F%2Fartists.spotify.com%2Fc")


## web automation
loginname = 'your login name'
loginpassword = 'your password'
artistname = 'your artist name'
spartistid = 'your artist urn'
sptrackidlist = list(artisttrack.id)
names = [re.sub('\W', '',list(artisttrack.name)[i]) for i in range(len(sptrackidlist))]


browser.find_element_by_xpath("//*[@id='login-username']").send_keys(loginname)
browser.find_element_by_xpath("//*[@id='login-password']").send_keys(loginpassword)
browser.find_element_by_xpath("/html/body/div[2]/div/form/div[3]/div[2]/button").click()
#time.sleep(10)

## go to stream of each song page
## the url is for the your artists and their work of music
#browser.get("https://creator.wg.spotify.com/artistinsights/v2/artist/64fJiKnU2RfnndB8xP3gLi/recording/4EIW00kTdPSUDOJwkN80x5/timeline/streams?time-filter=since2015")
#browser.get("https://creator.wg.spotify.com/artistinsights/v2/artist/64fJiKnU2RfnndB8xP3gLi/recording/4EIW00kTdPSUDOJwkN80x5/timeline/streams?time-filter=7day")

#select = Select(browser.find_element_by_xpath('//*[@id="timeline"]/div/div[1]/div/select'))
##select.select_by_index(index)
##select.select_by_visible_text("text")
#select.select_by_value('since2015')

def spotify(spartistid, sptrackid):
    streamurl= "https://creator.wg.spotify.com/artistinsights/v2/artist/"+spartistid+'/recording/'+sptrackid+"/timeline/streams?time-filter=since2015"
    songpageurl = "https://artists.spotify.com/c/artist/"+spartistid+'/song/'+sptrackid
    browser.get(streamurl)
    browser.get(songpageurl)#WebDriverWait(browser, 30).until(lambda x: x.find_element_by_xpath("//*[@id='timeline']/div/div[1]/div/select"))
    time.sleep(10)
#    WebDriverWait(browser, 100).until(EC.presence_of_element_located(browser.find_element_by_xpath('//*[@id="timeline"]/div/div[1]/div/select/option[3]')))
    select = Select(browser.find_element_by_class_name('select_menu__1jmJS'))
    select.select_by_value('since2015')
#    time.sleep(10)
#    select_menu__1jmJS
#    browser.find_element_by_xpath('//*[@id="timeline"]/div/div[1]/div').click()
#    browser.find_element_by_xpath('//*[@id="timeline"]/div/div[1]/div/select').click()
#    browser.switch_to.frame('iframe')
#    browser.find_element_by_xpath('//*[@id="timeline"]/div/div[1]/div/select/option[3]').click()
    time.sleep(5)
    browser.back()
    page = browser.page_source ## format daily pandora spin for each song
    soup= BeautifulSoup(page, 'html.parser')
    new=soup.text
    new= new[new.index('['):new.index(']')+1]
    new=json.loads(new)
    new=pd.DataFrame(new)  ##spotify stream for each song
    new.columns=['Date','SpotifyStream']
    return new[new.Date>='2017-01-01']
    

spdf=[i for i in range(len(sptrackidlist))]
for i in range(len(sptrackidlist)):
    spdf[i]=spotify(spartistid,sptrackidlist[i])


       
##initial ###

#//*[@id="timeline"]/div/div[1]/div/select/option[3]
#spotifystream(spartistid, sptrackidlist)

spotifydf_merged = reduce(lambda  left,right: pd.merge(left,right,on=['Date'],
                                                       how='outer'), spdf)
spotifydf_merged.columns=['Date']+names

### save spotify stream ###################
##names = [re.sub('\W', '',list(artisttrack.name)[i]) for i in range(len(sptrackidlist))]
#def save_xls(list_dfs, xls_path):
#    writer = pd.ExcelWriter(xls_path)
#    for i, df in enumerate(list_dfs):
#        df.to_excel(writer,sheet_name="{0}".format(names[i]),index=False)
#    writer.save()
   
thisweek = datetime.datetime.now().strftime("%Y-%m-%d")
#this_xlspath = '~/WeeklyStreamAnalysisPipeline/'+artistname + thisweek+'.xlsx'
#save_xls(new,this_xlspath)


#########################  pandora spin data pulling ####################################

browser2 = webdriver.Chrome('~/chromedriver_win32/chromedriver.exe')
browser2.get("https://www.nextbigsound.com/login")
browser2.find_element_by_xpath("//*[@id='email-box']").send_keys('your registered e-mail')
browser2.find_element_by_xpath("//*[@id='password-box']").send_keys('your password')
browser2.find_element_by_xpath("//*[@id='nbsBody']/div/section/nbs-login-view/section[2]/form/button").click()
WebDriverWait(browser2, 30).until(EC.presence_of_element_located((By.LINK_TEXT, "LOG OUT")))

#browser.get("https://www.nextbigsound.com/reports/231408")
#browser.get("https://www.nextbigsound.com/graphs/c/232767") ##$$%%
#time.sleep(10)
#browser.find_element_by_xpath("//*[@id='date']/section/header/input").clear()
#def days_between(d1, d2):
#    return abs((d2 - d1).days)
#uptonow = days_between(datetime.date.today(),datetime.date(2016,4,23))
#browser.find_element_by_xpath("//*[@id='date']/section/header/input").send_keys(uptonow)
#time.sleep(5)
#browser.find_element_by_xpath("//*[@id='report-header']/div/div[1]/div[2]/button[4]").click()

############ the page $$%% doesn't work, extract from backend #########
## this is an example for a specific artist, you could change him to your artist.
## valid link https://www.nextbigsound.com/api/data/tracks_geo/242517789/410/17043/17575/global
## pantrackids = [221959079,221967430,221978652,221939527,242517789,221959080,301366176,242214747]

pandoraidx=[['Stupid','242214747'],
 ['DontWasteTheNight','242517789'],
 ['LoveHealswithAlisonKrauss','301366176'],
 ['LoveYouHateYouMissYou', '221959079'],
 ['LifesForLivin','221978652'],
 ['WindowDownDays','221959080'],
 ['ChainReaction','221939527'],
 ['GutsAndGlory','221967430'],
 ['SongsWeSang','321702657']]
pandoraidx = pd.DataFrame(pandoraidx,columns=['song','idx'])
pantrackids = list(pandoraidx.idx)
#pandoralinks = list(map(lambda x : "https://www.nextbigsound.com/api/data/tracks_geo/"+x+"/410/17043/17575/global",pantrackids))
def days_between(d1, d2):
    return abs((d2 - d1).days)

today=days_between(datetime.date.today(),datetime.date(1970,1,1))

def pandora(pantrackid):
    pandoralink="https://www.nextbigsound.com/api/data/tracks_geo/"+pantrackid+"/410/17043/"+str(today)+"/global"
    browser2.get(pandoralink)
    pandorapage = browser2.page_source
    psoup = BeautifulSoup(pandorapage, 'html.parser')
    #extract data
    pandoradict=json.loads(psoup.text)
    pandoradict = pandoradict['data']['1285248']['410']['values'][pantrackid]
    pandoradf = pd.DataFrame(list(pandoradict.items()), columns=['Date', 'PandoraTotal'])
    #format df
    pandoradf.Date = list(map(lambda x: (datetime.date(1970,1,1)+datetime.timedelta(days=int(x))).strftime("%Y-%m-%d"), 
                              list(pandoradf.Date)))
    pandoradf["PandoraSpin"] = pandoradf["PandoraTotal"].diff(1)
#        pandoradf= pandoradf.drop(['PandoraTotal'],axis = 1, inplace = True)
    return pandoradf.iloc[:,[0,2]]


pddf=[i for i in range(len(pantrackids))]
for i in range(len(pantrackids)):
    pddf[i]=pandora(pantrackids[i])


pandoradf_merged = reduce(lambda  left,right: pd.merge(left,right,on=['Date'],
                                                       how='outer'), pddf)
pandoradf_merged.columns=['Date']+names

### save spotify_merge, pandora_merge
#mergedf_path = '~/WeeklyStreamAnalysisPipeline/merge_'+artistname + thisweek+'.xlsx'

mergedf_path = '~/WeeklyStreamAnalysisPipeline/merge_'+artistname+'.xlsx'
 
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer2 = pd.ExcelWriter(mergedf_path, engine='xlsxwriter')
pandoradf_merged.to_excel(writer2, sheet_name='pandora',index=False)
spotifydf_merged.to_excel(writer2, sheet_name='spotify',index=False)
writer2.save()
