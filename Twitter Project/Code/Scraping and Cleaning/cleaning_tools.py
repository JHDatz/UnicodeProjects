"""
Cleaning Tools

Holds all functions for converting scraped twitter data into
the filtered.csv and clean.csv files.
"""

import pandas as pd
import os
from joblib import dump, load

# For further cleaning with word stemming and lemmatization:
import string
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from pybaseball import pitching_stats, batting_stats

# Use get_current_date to write date for merged files

from datetime import date
from scraping_tools import get_current_date

### USED INFORMATION

### contraction_dict
###
### Used to remove contractions when cleaning out tweets.

contraction_dict = {"ain't": "is not", "aren't": "are not", "can't": "cannot", "'cause": "because",
                    "could've": "could have", "couldn't": "could not", "didn't": "did not", "doesn't": "does not",
                    "don't": "do not", "hadn't": "had not", "hasn't": "has not", "haven't": "have not",
                    "he'd": "he would", "he'll": "he will", "he's": "he is", "how'd": "how did",
                    "how'd'y": "how do you", "how'll": "how will", "how's": "how is", "I'd": "I would",
                    "I'd've": "I would have", "I'll": "I will", "I'll've": "I will have", "I'm": "I am",
                    "I've": "I have", "i'd": "i would", "i'd've": "i would have", "i'll": "i will",
                    "i'll've": "i will have", "i'm": "i am", "i've": "i have", "isn't": "is not", "it'd": "it would",
                    "it'd've": "it would have", "it'll": "it will", "it'll've": "it will have", "it's": "it is",
                    "let's": "let us", "ma'am": "madam", "mayn't": "may not", "might've": "might have",
                    "mightn't": "might not", "mightn't've": "might not have", "must've": "must have",
                    "mustn't": "must not", "mustn't've": "must not have", "needn't": "need not",
                    "needn't've": "need not have", "o'clock": "of the clock", "oughtn't": "ought not",
                    "oughtn't've": "ought not have", "shan't": "shall not", "sha'n't": "shall not",
                    "shan't've": "shall not have", "she'd": "she would", "she'd've": "she would have",
                    "she'll": "she will", "she'll've": "she will have", "she's": "she is", "should've": "should have",
                    "shouldn't": "should not", "shouldn't've": "should not have", "so've": "so have", "so's": "so as",
                    "this's": "this is", "that'd": "that would", "that'd've": "that would have", "that's": "that is",
                    "there'd": "there would", "there'd've": "there would have", "there's": "there is",
                    "here's": "here is", "they'd": "they would", "they'd've": "they would have", "they'll": "they will",
                    "they'll've": "they will have", "they're": "they are", "they've": "they have", "to've": "to have",
                    "wasn't": "was not", "we'd": "we would", "we'd've": "we would have", "we'll": "we will",
                    "we'll've": "we will have", "we're": "we are", "we've": "we have", "weren't": "were not",
                    "what'll": "what will", "what'll've": "what will have", "what're": "what are", "what's": "what is",
                    "what've": "what have", "when's": "when is", "when've": "when have", "where'd": "where did",
                    "where's": "where is", "where've": "where have", "who'll": "who will", "who'll've": "who will have",
                    "who's": "who is", "who've": "who have", "why's": "why is", "why've": "why have",
                    "will've": "will have", "won't": "will not", "won't've": "will not have", "would've": "would have",
                    "wouldn't": "would not", "wouldn't've": "would not have", "y'all": "you all",
                    "y'all'd": "you all would", "y'all'd've": "you all would have", "y'all're": "you all are",
                    "y'all've": "you all have", "you'd": "you would", "you'd've": "you would have",
                    "you'll": "you will", "you'll've": "you will have", "you're": "you are", "you've": "you have"}

# HELPER FUNCTIONS

# ifelse()
#
# A simple function meant to replicate the ifelse() function from the programming language R. Used with the apply()
# function for pandas dataframes.

def ifelse(boolean, ifValue, elseValue):
    if boolean:
        return ifValue
    else:
        return elseValue

### TEXT CLEANING TOOLS
###
### Used in the process of converting filtered.csv to clean.csv. All text information from tweets is converted into
### simpler words for the purposes of dimensionality reduction.
###
### There are currently two functions being used for this purpose: clean_text() and .

### _get_contractions()
###
### Written By: Jhagrut Lalwani
###
### Function which converts the contractions_dict dictionary into an elongated regular expression check.

def _get_contractions(contraction_dict):
    contraction_re = re.compile('(%s)' % '|'.join(contraction_dict.keys()))
    return contraction_dict, contraction_re

### replace_contractions(text)
###
### Written By: Jhagrut Lalwani
###
### Function which uses the elongated regular expression created by _get_contractions to replace contractions in text
### with their expanded set of words represented in the dictionary's values.

def replace_contractions(text):
    contractions, contractions_re = _get_contractions(contraction_dict)

    def replace(match):
        return contractions[match.group(0)]

    return contractions_re.sub(replace, text)

### clean_text(txt)
###
### Written By: Jhagrut Lalwani
###
### Original function for cleaning text back in August 2020. Takes a singular string as input and outputs a string which
### has been cleaned up. Removes contractions, punctuation, and stopwords. Function is applied to the 'clean' column of
### clean.csv.

def clean_text(txt):

    # replace contractions
    txt = replace_contractions(txt)
   
    #remove punctuations
    txt  = "".join([char for char in txt if char not in string.punctuation])
    txt = re.sub('[0-9]+', '', txt)
   
    # split into words
    words = word_tokenize(txt)
   
    # remove stopwords
    stop_words = set(stopwords.words('english'))
    words = [w for w in words if not w in stop_words]
   
    # removing leftover punctuations
    words = [word for word in words if word.isalpha()]
   
    cleaned_text = ' '.join(words)
    return cleaned_text

### remove_emojis
###
### Admittedly found on the internet.
###
### Function which removes 99% of emojis from tweets.

def remove_emojis(data):
    emoj = re.compile("["
                      u"\U0001F600-\U0001F64F"  # emoticons
                      u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                      u"\U0001F680-\U0001F6FF"  # transport & map symbols
                      u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                      u"\U00002500-\U00002BEF"  # chinese char
                      u"\U00002702-\U000027B0"
                      u"\U00002702-\U000027B0"
                      u"\U000024C2-\U0001F251"
                      u"\U0001f926-\U0001f937"
                      u"\U00010000-\U0010ffff"
                      u"\u2640-\u2642"
                      u"\u2600-\u2B55"
                      u"\u200d"
                      u"\u23cf"
                      u"\u23e9"
                      u"\u231a"
                      u"\ufe0f"  # dingbats
                      u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, ' emojiToken ', data)

### cleaning_function_part_1(x)
###
### Written By: Joe Datz
###
### Function which takes a string as input and removes/replaces:
###
###     1. Twitter Accounts with a token
###     2. Hyperlinks with a token
###     3. Years with a token
###     4. Phone Numbers with a token
###     5. Pound Symbols from hashtags
###     6. Ampersands and "w/" symbols
###     7. Times with a token
###     8. Contractions
###
### This function is listed as "part 1" because it is used in tandem with cleaning_function_part_1().

def cleaning_function_part_1(x):
    new = re.sub("@[A-Za-z0-9]+", " accountToken ", x)  # Replace twitter accounts with token
    new = re.sub(r"http\S+", " hyperlinkToken ", new)  # Replace hyperlinks with token
    new = re.sub(" 20[0-3][0-9] ", ' yearToken ', new)  # Replace year with token
    new = re.sub("[0-9]{3}-[0-9]{3}-[0-9]{4}|\([0-9]{3}\)[0-9]{3}-[0-9]{4}|[0-9]{3}\.[0-9]{3}\.[0-9]{4}",
                 '  phoneNumberToken  ', new)  # Replace phone number with token
    new = re.sub(" [0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4} ", " dateToken ", new)
    new = remove_emojis(new)
    new = new.replace('#', '')  # Remove pound symbol from hashtags
    new = new.replace('&amp;', ' and ')  # Remove ampersand symbol
    new = new.replace('w/', ' with ')  # Remove w/ with "with"
    new = new.lower()  # All lowercase to reduce duplicates with capitalization
    new = re.sub("[0-9]:[0-9]{2}am|[0-9]:[0-9]{2}pm|[0-9][0-9]{2}am|[0-9][0-9]{2}pm|[0-9]:[0-9]{2}|[0-9]am|[0-9]pm",
                 ' timetoken ', new)  # replace time with token - easier with lowercase
    new = replace_contractions(new)  # remove contractions - must be after setting everything to lower case

    return new

### cleaning_function_part_2(x)
###
### Written By: Joe Datz
###
### Function which replicates part of clean_text() and does the following:
###
###     1. Remove punctuation
###     2. Remove stopwords
###     3. Replace words with their stemmed version
###
### This function is listed as "part 1" because it is used in tandem with cleaning_function_part_1().

def cleaning_function_part_2(x):
    # Remove punctuation

    new = "".join([char for char in x if char not in string.punctuation])
    new = re.sub('[0-9]+', ' ', new)

    # split into words
    words = word_tokenize(new)

    # remove stopwords
    stop_words = set(stopwords.words('english'))
    ps = PorterStemmer()
    words = [ps.stem(w) for w in words if not w in stop_words]

    # removing leftover punctuations
    words = [word for word in words if word.isalpha()]

    new = ' '.join(words)

    return new

### cleaning_total()
###
### Written By: Joe Datz
###
### Function which reduces text from tweets by combining cleaning_function_part_1() and cleaning_function_part_2() as
### well as removing names from the dataset. These are represented in the "clean2" column and "clean2_no_names" column
### of clean.csv.

def cleaning_total():
    data = pd.read_csv('clean.csv')
    data['clean2'] = data['tweet'].apply(cleaning_function_part_1)

    names = [name.lower() for name in
             set(pitching_stats(2015, 2021)['Name']).union(set(batting_stats(2015, 2021)['Name']))]
    lastnames = [name.split(' ')[1] for name in names]

    for i in range(len(names)):
        data['clean2_no_names'] = data['clean2'].apply(lambda x: x.replace(names[i].lower(), ' baseballplayertoken '))
        data['clean2_no_names'] = data['clean2'].apply(lambda x: x.replace(lastnames[i].lower(), ' baseballlastnametoken '))

    data['clean2'] = data['clean2'].apply(cleaning_function_part_2)
    data['clean2_no_names'] = data['clean2'].apply(cleaning_function_part_2)

    data.to_csv('clean.csv')

### FILE CONVERTING FUNCTIONS
###
### The purpose of these functions is take raw data taken from the twint library and convert them into large files.
### The 'merged' files are aggregations of unedited data collected from twint, the 'filtered2' file is aggregation of
### 'merged' files to remove duplicates and update with the latest like / retweet count for each unique tweet, and the
### 'clean' file is the text data of tweets with duplicate text information also removed (tweets are unique but tweet
### text is not unique).

### scrape_to_merge():
###
### This function takes all files created with the twint library during its tweet search and converts it into a single
### 'merged' file. All information is completely unedited during this process.
###
### There are multiple 'merged' files because loading all tweets into memory (especially considering a raspberry pi does
### this) would be impossible without breaking the files into chunks. This function handles the breaking of files into
### chunks by:
###
###     1. Each new merged file (excluding older ones) is divided by the date of the scrape.
###     2. After 2 million entries the data of a particular date is broken off into a second file.
###
### Once done, the data is converted into a file which has the format 'merged (file number here) (date here).csv'.
### The older files before this standard are merged.csv, merged2.csv, merged3.csv and 'InsideInjuries merged.csv'.

def scrape_to_merge():
    fileList = [os.getcwd()  + '/TweetData/' + files 
                for files in os.listdir(os.getcwd()  + '/TweetData')]

    data = pd.DataFrame()
    counter = 1
    
    for i in range(len(fileList)):
        data = data.append(pd.read_csv(fileList[i]))
        if data.shape[0] > 2000000:
            data.drop_duplicates()
            data.to_csv('merged ' + str(counter) +  ' ' + get_current_date() + '.csv', index=False)
            counter = counter + 1
            data = pd.DataFrame()
        
    data = data.drop_duplicates()
    data.to_csv('merged ' + str(counter) +  ' ' + get_current_date() + '.csv', index=False)
    del data

### aggregate_merged()
###
### aggregated_merged has two functions depending on the variable "mergetype":
###
###     1. If mergetype == 0: All the merged files are put together (done on laptop to avoid memory problems on pi).
###     2. If mergetype == 1: All data is given boolean columns to denote whether or not it contains a url, photo, or
###         retweet and then its information regarding replies, retweets, or likes is updated to the latest version.
###
### This function is used inside the function merged_to_filtered() to reduced the raw data in the "merged" fileset into
### a single file called "filtered2.csv".

def aggregate_merged(file, mergetype):
    if mergetype == 1:

        data = pd.read_csv(file)
        data['urls'] = data['urls'].apply(lambda x: str(x).lstrip("[").rstrip("]"))
        data['link_present'] = data['urls'].apply(lambda x: ifelse(len(x) > 0, 1, 0))
        data['photos'] = data['photos'].apply(lambda x: str(x).lstrip("[").rstrip("]"))
        data['photo_present'] = data['photos'].apply(lambda x: ifelse(len(x) > 0, 1, 0))
        data['retweet'] = data['retweet'].astype(bool).apply(lambda x: ifelse(x, 1, 0))

    else:

        data = pd.concat(file)

    final = data.groupby(['link', 'tweet']).agg({'replies_count': 'max', 'retweets_count': 'max', 'likes_count': 'max',
                                                 'link_present': 'max', 'photo_present': 'max', 'retweet': 'max'})

    final['replies_count'] = final['replies_count'].astype(int)
    final['retweets_count'] = final['retweets_count'].astype(int)
    final['likes_count'] = final['likes_count'].astype(int)
    final['link_present'] = final['link_present'].astype(int)
    final['photo_present'] = final['photo_present'].astype(int)
    final['retweet'] = final['retweet'].astype(int)

    final = final.reset_index(0).reset_index(0)

    return final

### append_labels()
###
### This function takes newly labeled information from get_data_to_label() functions or corrected data from
### gather_fns_and_fps() and adds them into filtered2.csv. These are left-joined onto filtered.csv and then
### a new column is updated out of the two created from the join. This file is then returned to the original functions
### it was called from.

def append_labels(data, file):

    # Left join onto existing filtered2.csv file after removing duplicates and empty rows.

    labeled = pd.read_csv(file)
    labeled = labeled[['injury_report', 'tweet']]
    labeled = labeled[labeled['injury_report'] != 'x']
    labeled = labeled.drop_duplicates()
    labeled = labeled.dropna()
    filtered = data.merge(labeled, on='tweet', how='left')

    try:
        filtered['injury_report'] = filtered['injury_report_y'].fillna(filtered['injury_report_x']).fillna('x')
        filtered.drop(['injury_report_x', 'injury_report_y'], axis=1, inplace=True)
    except KeyError:
        filtered['injury_report'] = filtered['injury_report'].fillna('x')
    return filtered

### merged_to_filtered()
###
### This function aggregates all merged files into one pandas dataframe, reduces its size to the columns of
### filtered2.csv, adds it to filtered.csv, and then updates the CSV file.

def merged_to_filtered():
    file_aggregates = [aggregate_merged(filename, 1) for filename in os.listdir() if 'merged' in filename]
    filtered = aggregate_merged(file_aggregates, 0)
    filtered = append_labels(filtered, 'filtered2.csv')
    filtered.to_csv('filtered2.csv', index=False)

### label_filtered_duplicates():
###
### A rarely-used function meant to make sure non-unique tweet text is labeled if one of its copies has already been
### labeled.

def label_filtered_duplicates():
    filtered = pd.read_csv('filtered2.csv')
    labeled_data = filtered[filtered['injury_report'] != 'x'][['tweet', 'injury_report']].drop_duplicates()
    labeled_data.to_csv('copy.csv', index=False)
    filtered = append_labels(filtered, 'copy.csv')
    filtered.to_csv('filtered2.csv', index=False)

### filtered_to_clean():
###
### A function which converts filtered2.csv into clean.csv, the file used for training the models. clean.csv contains
### just 4 columns:
###
###     1. injury_report - the label
###     2. tweet - the unedited tweet text
###     3. clean - the tweet when modified the clean_text() function.
###     4. clean2 - the tweet when modified by the cleaning_total() function, but names are kept.
###     5. clean2_no_name - the tweet when the tweet when modified by the cleaning_total() function, but names aren't kept.
    
def filtered_to_clean():
    data = pd.read_csv('filtered2.csv')
    data = data[data['injury_report'] != 'x']
    data = data[['injury_report', 'tweet']]
    data = data.drop_duplicates()
    data = data[data['tweet'].apply(lambda x: isinstance(x, str))]
    data['clean'] = data['tweet'].apply(lambda txt: clean_text(txt))
    data.dropna(inplace = True)
    data.to_csv('clean.csv', index=False)
    cleaning_total()
    del data

### get_data_to_label()
###
### A function which serves two purposes:
###
###     1. Randomly find 1000 datapoints to label.
###     2. Randomly take 100,000 tweets and use logistic regression to find positive cases to label. This produces new
###         datapoints which are disproportionately class 1 - otherwise it would be difficult to find positive tweets.
    
def get_data_to_label():
    data = pd.read_csv('filtered2.csv')
    data = data[data['injury_report'] == 'x']
    data = data[['injury_report', 'tweet']]
    data.drop_duplicates(inplace = True)
    data.dropna(inplace = True)
    
    samples_to_label = data.sample(1000)
    samples_to_label.to_csv('sampled.csv')

    lgr = load('Classical Models//logistic_regression.joblib')
    v_tfidf = load('Classical Models//tfidf.joblib')

    data = data.sample(100000)
    data['clean'] = data['tweet'].apply(lambda txt: clean_text(txt))
    data['lgr_predictions'] = lgr.predict(v_tfidf.transform(data['clean']))
    positives = data[data['lgr_predictions'] == 1]
    positives = positives[['injury_report', 'tweet']]
    positives.to_csv('positive_samples.csv')
    del data

### label_new_data()
###
### Short function which takes all samples generated from the get_data_to_label() function and adds them to filtered2.csv.

def label_new_data():
    filtered = pd.read_csv('filtered2.csv')
    filtered = append_labels(filtered, 'sampled.csv')
    filtered = append_labels(filtered, 'positive_samples.csv')
    filtered.to_csv('filtered2.csv', index=False)

### gather_fns_and_fps
###
### Extended function for finding all false positives and false negatives in our dataset from our models - typically
### we might find mislabelings here. All classical models are loaded, and then their outputs are recorded into csv files
### which are checked.

def gather_fns_and_fps(filename):
    # load all models and transforming functions.

    classical_models = [[entry.split('.')[0], load('Classical Models\\' + entry)]
                        for entry in os.listdir('Classical Models')
                        if entry not in ['bool.joblib', 'count.joblib', 'tfidf.joblib']]

    v_bool = load('Classical Models\\bool.joblib')
    v_tfidf = load('Classical Models\\tfidf.joblib')
    v_count = load('Classical Models\\count.joblib')

    data = pd.read_csv(filename)

    # if we're starting from filtered.csv, we'll need to drop duplicates and create the clean column first.

    if filename == 'filtered2.csv':
        data = data[data['injury_report'] != 'x']
        data = data[['injury_report', 'tweet']]
        data.drop_duplicates(inplace=True)
        data.dropna(inplace=True)
        data['clean'] = data['tweet'].apply(lambda txt: clean_text(txt))

    # Get all false positives and false negatives and send them to files for observation.

    for models in classical_models:

        if models[0] in ['bernoulliNB', 'gradient_boosting', 'kNN_bool', 'random_forest_bool']:

            data[models[0] + '_predictions'] = models[1].predict(v_bool.transform(data['clean']))
            data[models[0] + '_fns'] = ((data['injury_report'].astype(int) == 1) &
                                        (data[models[0] + '_predictions'] == 0)).astype(int)
            data[models[0] + '_fps'] = ((data['injury_report'].astype(int) == 0) &
                                        (data[models[0] + '_predictions'] == 1)).astype(int)

            data[data[models[0] + '_fns'] == 1][['injury_report', 'tweet', 'clean']].to_csv(
                'fns and fps\\' + models[0] + '_fns.csv', index=False)

            data[data[models[0] + '_fps'] == 1][['injury_report', 'tweet', 'clean']].to_csv(
                'fns and fps\\' + models[0] + '_fps.csv', index=False)


        elif models[0] in ['kNN_tfidf', 'logistic_regression', 'random_forest_tfidf', 'svm']:

            data[models[0] + '_predictions'] = models[1].predict(v_tfidf.transform(data['clean']))
            data[models[0] + '_fns'] = ((data['injury_report'].astype(int) == 1) &
                                        (data[models[0] + '_predictions'] == 0)).astype(int)
            data[models[0] + '_fps'] = ((data['injury_report'].astype(int) == 0) &
                                        (data[models[0] + '_predictions'] == 1)).astype(int)

            data[data[models[0] + '_fns'] == 1][['injury_report', 'tweet', 'clean']].to_csv(
                'fns and fps\\' + models[0] + '_fns.csv', index=False)

            data[data[models[0] + '_fps'] == 1][['injury_report', 'tweet', 'clean']].to_csv(
                'fns and fps\\' + models[0] + '_fps.csv', index=False)

        else:

            data[models[0] + '_predictions'] = models[1].predict(v_count.transform(data['clean']))
            data[models[0] + '_fns'] = ((data['injury_report'].astype(int) == 1) &
                                        (data[models[0] + '_predictions'] == 0)).astype(int)
            data[models[0] + '_fps'] = ((data['injury_report'].astype(int) == 0) &
                                        (data[models[0] + '_predictions'] == 1)).astype(int)

            data[data[models[0] + '_fns'] == 1][['injury_report', 'tweet', 'clean']].to_csv(
                'fns and fps\\' + models[0] + '_fns.csv', index=False)

            data[data[models[0] + '_fps'] == 1][['injury_report', 'tweet', 'clean']].to_csv(
                'fns and fps\\' + models[0] + '_fps.csv', index=False)

    # Create a file combining all false positives and false negatives.
    # We specifically exclude the kNN due to poor performance.

    fps = pd.concat([pd.read_csv('fns and fps\\' + files) for files in os.listdir('fns and fps')
                     if 'fps' in files and 'total' not in files and 'kNN' not in files], axis=0)
    fps.drop_duplicates(inplace=True)
    fps.to_csv('fns and fps\\fps_total.csv', index=False)

    fns = pd.concat([pd.read_csv('fns and fps\\' + files) for files in os.listdir('fns and fps')
                     if 'fns' in files and 'total' not in files and 'kNN' not in files], axis=0)
    fns.drop_duplicates(inplace=True)
    fns.to_csv('fns and fps\\fns_total.csv', index=False)
    