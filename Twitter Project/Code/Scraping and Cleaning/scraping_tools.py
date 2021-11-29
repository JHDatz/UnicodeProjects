"""
Scraping Tools

Provides all tools for getting data onto/off the pi and submitting back to google drive.
"""

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import requests
import time
import os
import shutil

# for twint

import twint
import nest_asyncio
from datetime import date

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

    with open('download_ids_and_locations.csv') as file:
        
        ids_and_locations = [line.rstrip('\n').split(',') 
        for line in file.readlines()]
    
        for i in range(len(ids_and_locations)):
            file_id = ids_and_locations[i][0]
            destination = ids_and_locations[i][1]
            download_file_from_google_drive(file_id, destination)
    
def upload_files():
    gauth = GoogleAuth()          
    drive = GoogleDrive(gauth)

    with open('upload_ids_and_locations.csv') as file:
        
        ids_and_locations = [line.rstrip('\n').split(',') 
        for line in file.readlines()]
    
        for i in range(len(ids_and_locations)):
    
            gfile = drive.CreateFile({'parents': [{'id': ids_and_locations[i][0]}],
                                      'id': ids_and_locations[i][1]})

            file_location = ids_and_locations[i][2]
            filename = ids_and_locations[i][2].split('/')
            filename = filename[len(filename)-1]
            cwd = os.getcwd()
            new_file_location = cwd + '\\' + filename
            starting_file_location = file_location.rstrip(filename)

            if 'Classical Models' in file_location:
                os.chdir(os.getcwd() + '\\' + 'Classical Models')
                gfile.SetContentFile(filename)
                gfile.Upload()
                os.chdir(os.getcwd().rstrip('\\Classical Models'))
            else:
                gfile.SetContentFile(filename)
                gfile.Upload()

            time.sleep(5)

def get_file_list(fileName):
    file = open(fileName)
    text = file.readlines()
    text = [line.rstrip('\n') for line in text]
    file.close()
    return text

def write_file_list(list, filename):
    file = open(filename, 'w')

    for entry in list:
        file.write(entry + '\n')
    file.close()

def get_current_date(): return str(date.today().year) + '-' + str(date.today().month) + '-' + str(date.today().day)
    
def scrape_twitter_accounts():
    
    nest_asyncio.apply()

    userids = get_file_list('lists\\accountList.txt')
    broken_ids = get_file_list('lists\\brokenList.txt')
    current_date = get_current_date()
    count=0
    
    while count < len(userids) - 1:
        
        if count % 250 == 0: print(count, 'usernames reached.')
        
        try:
            c = twint.Config()
            c.Username = userids[count]
            c.Limit = 1000
            c.Store_csv = True
            c.Since = current_date
            c.Output = 'TweetData/' + userids[count] + ".csv"
            c.Hide_output = True
            
            twint.run.Search(c)
            del c
            time.sleep(15)
            count+=1
            
        except ValueError:
            broken_ids.append(userids[count])
            
        count+=1

    write_file_list(set(userids) - set(broken_ids), 'lists\\accountList.txt')
    write_file_list(set(broken_ids), 'lists\\brokenList.txt')

def scrape_twitter_hashtags():

    nest_asyncio.apply()

    hashtagList = get_file_list('lists\\hashtagList.txt')
    current_date = get_current_date()

    for hashtags in hashtagList:

        c = twint.Config()
        c.Search = hashtags
        c.Since = current_date
        c.Lang = 'en'
        c.Store_csv = True
        c.Output = 'TweetData/' + hashtags + '.csv'
        c.Hide_output = True

        twint.run.Search(c)
        time.sleep(15)