#!/usr/bin/env python
import os
import json

from twitter import Api

# Custom import
from datetime import datetime
from datetime import date
import time
import re
import sys

def loadConfig(config_secret):
    # Go to http://apps.twitter.com and create an app.
    # The consumer key and secret will be generated for you after
    global CONSUMER_KEY
    global CONSUMER_SECRET
    # After the step above, you will be redirected to your app's page.
    # Create an access token under the the "Your access token" section
    global ACCESS_TOKEN
    global ACCESS_TOKEN_SECRET
    with open(config_secret, 'r') as cred:
        json_str = cred.read()
        json_data = json.loads(json_str)
        CONSUMER_KEY = json_data['consumer_key']
        CONSUMER_SECRET = json_data['consumer_secret']
        ACCESS_TOKEN = json_data['access_token']
        ACCESS_TOKEN_SECRET = json_data['access_token_secret']

# Users to watch for should be a list. This will be joined by Twitter and the
# data returned will be for any tweet mentioning:
# @twitter *OR* @twitterapi *OR* @support.
#USERS = ['@twitter', '@twitterapi', '@support']
LOCATIONS = ['-6.38','49.87','1.77','55.81']
UK = ['-5.95459','49.979488','-0.109863','58.12432']            # United Kingdom
US = ['-123.960279', '33.080519', '-60.996094', '45.336702']    # US
AU = ['105.785815', '-44.513723', '154.301442', '-12.449423']   # Australia
NZ = ['164.772949', '-47.15984', '179.626465', '-33.94336']     # New Zealand
SEA = ['90.825760', '-11.836210', '153.766943', '21.217420']    # South East Asian
AF = ['-25.195408', '-35.880958', '32.812407', '31.960635']     # African
COUNTRIES = ['UK', 'US', 'AU', 'NZ', 'SEA', 'AF']
DAY_CYCLE = 2

def getLocation(country_code):
    if country_code == 'UK':
        return UK, 0
    elif country_code == 'US':
        return US, 1
    elif country_code == 'AU':
        return AU, 2
    elif country_code == 'NZ':
        return NZ, 3
    elif country_code == 'SEA':
        return SEA, 4
    elif country_code == 'AF':
        return AF, 5
    else:
        return UK, 0
        
def write_to_file(filename, text, append=True):
    if append:
        mode = 'a'
    else:
        mode = 'w'
    with open(filename, mode) as fw:
        fw.write(str(text) + '\n')
        
def normalize_tweet_text(tweet_text):
    # Normalize text
    ## Remove comma, linefeed, and tab
    tweet_text = re.sub('[,\n\t]', ' ', tweet_text)
    ## Remove http link from tweet_text
    tweet_text = re.sub('http?([-a-zA-Z0-9@:%_\+.~#?&//=])*', ' ', tweet_text)
    ## Remove multiple spaces
    tweet_text = re.sub(' +',' ',tweet_text)
    ## Encode special character to utf-8 format, because ASCII is sucks (can't support wide range of characters)
    tweet_text = tweet_text.encode('utf-8','ignore')
    tweet_text = str(tweet_text)
    return tweet_text
        
def extract_line(directory, country_code, count, count_thousands, today, line):
    try:
        try:
            lang = line['lang'] # String
            # English only
            if lang != 'en':
                return
        except:
            pass
        # Extract line information
        try:
            geo = line['geo'] # String
        except Exception as ex:
            #print 'Geo Exception %s' % ex
            return
        #geo = line['geo'] # Object
        timestamp_ms = line['timestamp_ms'] # Long Integer
        user = line['user'] # Object
        #entities = line['entities'] # Object
        tweet_id = line['id'] # Integer
        tweet_text = line['text'] # String
        retweet_count = line['retweet_count']
        # Extract user information
        user_id = user['id'] # Integer
        utc_offset = user['utc_offset'] # Integer
        if utc_offset is None:
            utc_offset = ''
        else :
            utc_offset = str(utc_offset).strip()
        #friends_count = user['friends_count'] # Integer
        #followers_count = user['followers_count'] # Integer
        #statuses_count = user['statuses_count'] # Integer
        # Extract entities information
        #hashtags = entities['hashtags'] # Array of String
        #user_mentions = entities['user_mentions'] # Dictionary
        # Extract user_mentions information
        #for user_mention in user_mentions:
        #    mentioned_id = user_mention['id']
            #print(str(mentioned_id)+'\n')
        # Print for testing
        #print(str(geo))
        #print(str(timestamp_ms))
        #print(str(user_id))
        #print(str(entities))
        #print(str(tweet_id))
        # For each geotagged tweets
        if geo is not None:
            #print(str(geo))
            try:
                coordinates = geo['coordinates'] # Array of Float
                gps = []
                for var in coordinates:
                    gps.append(str(var))
            except Exception as ex:
                print 'Coordinate Exception %s' % ex
                return
            #print gps[0]
            #print gps[1]
            # Normalize text
            tweet_text = normalize_tweet_text(tweet_text)
            # Write all logs
            f_summary  = '{0}logs/summary_{1}_{2}.csv'.format(directory, country_code, today) 
            csv_output = '{0},{1},{2},{3},{4},{5},{6}'.format(tweet_id, user_id, timestamp_ms, gps[0], gps[1], tweet_text, utc_offset)
            if csv_output != '':
                write_to_file(f_summary, csv_output)
        #time.sleep(1)
    except Exception as ex:
        f_error = '{0}logs/error_{1}.txt'.format(directory, today)
        with open(f_error, 'a') as fw:
            fw.write('[{0}] Extract Exception {1}\n'.format(str(datetime.now()),ex))
            fw.write('[{0}] {1}\n'.format(str(datetime.now()),line))

##########################
# Main function
##########################

def main():
    arglen = len(sys.argv)
    USING_TWITTER = False
    if arglen == 3:
        directory = sys.argv[1]
        country_code = sys.argv[2]
        LOCATIONS, selected = getLocation(country_code)
        USING_TWITTER = True
        #print country_code
        #print LOCATIONS
        #print selected
    elif arglen == 2:
        directory = sys.argv[1]
    else :
        print 'Please give two inputs: directory name and country code {US, UK, AU, NZ, SEA, AF}'
        return
    if directory != '':
        directory = directory + '/'
    if USING_TWITTER:
        loadConfig('config_secret.json')
        # Since we're going to be using a streaming endpoint, there is no need to worry
        # about rate limits.
        api = Api(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        # api.GetStreamFilter will return a generator that yields one status
        # message (i.e., Tweet) at a time as a JSON dictionary.
    try:
        if USING_TWITTER:
            today = date.today()
            count_day = 0
            counter = 0
            count_thousands = 0
            print country_code
            print today
            for line in api.GetStreamFilter(locations=LOCATIONS, stall_warnings=True):
                try:
                    if date.today() != today :
                        # Change day
                        today = date.today()
                        try:
                            print('--- End of the day ---')
                            print('[{0}] Processed {1:,} tweets'.format(str(datetime.now()),counter))
                        except:
                            pass
                        counter = 0
                        count_thousands = 0
                        count_day += 1
                        if count_day == DAY_CYCLE:
                            count_day = 0
                            # Change the countries
                            selected = (selected + 1 ) % len(COUNTRIES)
                            country_code = COUNTRIES[selected]
                            LOCATIONS, selected = getLocation(country_code)
                            print country_code
                        print today
                    # Write json to file
                    f_complete = '{0}logs/log_{1}_{2}.txt'.format(directory, country_code, today)
                    #print json.dumps(line)
                    write_to_file(f_complete, json.dumps(line))
                    # Counter
                    counter = counter + 1
                    if counter % 1000 == 0 and counter > 0:
                        counter = 0
                        count_thousands = count_thousands + 1
                        print('[{0}] Processed {1},000 tweets'.format(str(datetime.now()),count_thousands))
                # except TwitterError as te:
                #     f_error = '{0}logs/error_{1}.txt'.format(directory, str(today))
                #     write_to_file(f_error, '[{0}] Twitter Error: {1}\n'.format(str(datetime.now()),ex))
                except Exception as ex:
                    f_error = '{0}logs/error_{1}.txt'.format(directory, str(today))
                    with open(f_error, 'a') as fw:
                        fw.write('[{0}] Line Exception {1}\n'.format(str(datetime.now()),ex))
                        fw.write('[{0}] {1}\n'.format(str(datetime.now()),line))
        else:
            # Loop through os files
            # and create similar filename but using csv
            # Extract json and write into csv file
            #extract_line(directory, country_code, counter, count_thousands, today, line)
            pass
    except Exception as ex:
        f_error = '{0}logs/error_{1}.txt'.format(directory, str(today))
        write_to_file(f_error, '[{0}] Outer Exception {1}\n'.format(str(datetime.now()),ex))
        
##########################
# End of Main
##########################

if __name__ == '__main__':
    main()