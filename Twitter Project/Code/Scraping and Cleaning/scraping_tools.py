#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraping Tools

Provides all tools for getting data onto the pi and submitting back to google drive.
"""

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import requests
import time

# for twint

import twint
import nest_asyncio

def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                
def download_files():
    
    file_id = '1dFLBjiRkDtuDVDZWIKLr9mJHN2IBOiEn'
    destination = '/home/pi/Desktop/coding/python/Twitter Project/merged.csv'
    download_file_from_google_drive(file_id, destination)
    
    file_id = '1aZZWt7Qa-iRLf5E8L7PcrHkEeF9Jthjt'
    destination = '/home/pi/Desktop/coding/python/Twitter Project/filtered.csv'
    download_file_from_google_drive(file_id, destination)
    
    file_id = '1GqA10Vb3CkLqJcXnXB_RmnPP34GJZzGs'
    destination = '/home/pi/Desktop/coding/python/Twitter Project/clean.csv'
    download_file_from_google_drive(file_id, destination)
    
    file_id = '1vsT-1FT_d6EkVxLYmOqM0grmPCI2PnT4'
    destination = '/home/pi/Desktop/coding/python/Twitter Project/accountList.txt'
    download_file_from_google_drive(file_id, destination)
    
    file_id = '1p4qokPg_5Br1YH3_blM9pkonLPg4WTSO'
    destination = '/home/pi/Desktop/coding/python/Twitter Project/brokenList.txt'
    download_file_from_google_drive(file_id, destination)
    
    file_id = '1HZAKkQuSoIkhPVZJfyDvnq3ziIIRD5BK'
    destination = '/home/pi/Desktop/coding/python/Twitter Project/labeledTweets.txt'
    download_file_from_google_drive(file_id, destination)
    
def upload_files():
    gauth = GoogleAuth()          
    drive = GoogleDrive(gauth)
    
    gfile = drive.CreateFile({'parents': [{'id': '1WJz-7MLXT1B1CyecyuMzPCJKhp8LrHh0'}],
                              'id': '1dFLBjiRkDtuDVDZWIKLr9mJHN2IBOiEn'})
    
    gfile.SetContentFile('merged.csv')
    gfile.Upload()
    time.sleep(5)
    
    gfile = drive.CreateFile({'parents': [{'id': '1WJz-7MLXT1B1CyecyuMzPCJKhp8LrHh0'}],
                              'id': '1aZZWt7Qa-iRLf5E8L7PcrHkEeF9Jthjt'})
    
    gfile.SetContentFile('filtered.csv')
    gfile.Upload()
    time.sleep(5)
    
    gfile = drive.CreateFile({'parents': [{'id': '1WJz-7MLXT1B1CyecyuMzPCJKhp8LrHh0'}],
                              'id': '1GqA10Vb3CkLqJcXnXB_RmnPP34GJZzGs'})
    
    gfile.SetContentFile('clean.csv')
    gfile.Upload()
    time.sleep(5)
    
def scrape_twitter():
    
    nest_asyncio.apply()

    file = open('accountList.txt')
    text = file.readlines()
    file.close()
    userids = [userid.strip('\n') for userid in text]
    broken_ids = list()
    count=0
    
    while count < len(userids) - 1:
        
        if count % 250 == 0: print(count, 'usernames reached.')
        
        try:
            c = twint.Config()
            c.Username = userids[count]
            c.Limit = 500
            c.Store_csv = True
            c.Output = 'TweetData/' + userids[count] + ".csv"
            c.Hide_output = True
            
            twint.run.Search(c)
            time.sleep(15)
            count+=1
            
        except ValueError:
            broken_ids.append(userids[count])
            
        count+=1