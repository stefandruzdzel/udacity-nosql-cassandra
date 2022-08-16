#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checks current working directory
print(os.getcwd())

# Get current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Creates a list of files and collects each filepath
for root, dirs, files in os.walk(filepath):
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in the csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[5]:


# This makes a connection to a Cassandra instance on your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[6]:


# Creates a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


# Sets KEYSPACE to the keyspace created above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[8]:


## Query 1:  Artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
## When designing a NoSQL database, it's important to think of the query first.

query1 = """SELECT artist, song, length
FROM song_in_session
WHERE sessionId = 338 AND itemInSession = 4"""

""" Table explanation:
The above query indicates that we'll need sessionId and itemInSession included in the Primary Key
The information that we want returned is the artist, song and length
So in total we'll create 5 columns: the 2 Primary Key columns, and the 3 data columns
Using "IF NOT EXISTS" to ensure we haven't already made a table with this name
"""
query = """CREATE TABLE IF NOT EXISTS song_in_session 
    (sessionId int,
    itemInSession int,
    artist text,
    song text,
    length float,
    PRIMARY KEY (sessionId, itemInSession))"""

try:
    session.execute(query)
except Exception as e:
    print(e)

                    


# In[9]:



# Opens the previously created data file
file = 'event_datafile_new.csv'
df = pd.read_csv(file)

# Query for inserting data to table
query = "INSERT INTO song_in_session (sessionId, itemInSession, artist, song, length)"
query = query + " VALUES (%s, %s, %s, %s, %s)"

# Loops through the DataFrame and inserts each row into the table
for i,row in df.iterrows():
    session.execute(query, (row['sessionId'],row['itemInSession'],row['artist'],row['song'],row['length']))

    


# #### Do a SELECT to verify that the data have been inserted into each table

# In[10]:


# Verify the data was entered into the table
# This is the query that we designed the table around
query1 = """SELECT artist, song, length
FROM song_in_session
WHERE sessionId = 338 AND itemInSession = 4"""

try:
    rows = session.execute(query1)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song, row.length)


# ### Question 2: Songs in a user session

# In[23]:


# Question 2: Name of artist, song (sorted by itemInSession) and user (first and last name)\
# for userid = 10, sessionid = 182

# This query will once again help design our table:
query2 = """SELECT artist, song, firstName, lastName
FROM songs_in_user_session
WHERE userid = 10 AND sessionId = 182"""

query = "DROP TABLE IF EXISTS songs_in_user_session"
try:
    session.execute(query)
except Exception as e:
    print(e)

""" Table Explanation:
Since our WHERE statement will check for a userid and sessionId, these will become our partition key
The data needs to be sorted by itemInSession, so this will be used as a clustering column
The query asks us to return the artist, song, firstName and lastName, so in total we have 7 columns.
Using "IF NOT EXISTS" since it's best practice.
"""
query = """CREATE TABLE IF NOT EXISTS songs_in_user_session (
    userid int,
    sessionId int,
    itemInSession int,
    artist text,
    song text,
    firstName text,
    lastName text,    
    PRIMARY KEY ((userid, sessionId), itemInSession))"""

try:
    session.execute(query)
except Exception as e:
    print(e)
    
# Opens the previously created data file
file = 'event_datafile_new.csv'
df = pd.read_csv(file)

#Query for inserting data to table
query = "INSERT INTO songs_in_user_session(userid, sessionId, itemInSession, artist, song, firstName, lastName)"
query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"

# Loops through the DataFrame and inserts each row into the table
for i,row in df.iterrows():
    session.execute(query, (int(row['userId']),int(row['sessionId']),int(row['itemInSession']),row['artist'],row['song'],row['firstName'],row['lastName']))

                    


# In[24]:


try:
    rows = session.execute(query2)
except Exception as e:
    print(e)
    
for row in rows:
    print (row)
                    


# ### Question 3: Users who listened to a song

# In[ ]:


##Question 3: Every user name (first and last) in the music app history who listened to the song 'All Hands Against His Own'
query3 = """SELECT firstName, lastName
    FROM song_listeners
    WHERE song = 'All Hands Against His Own'"""

""" Table Explanation:
Since we're searching by Song we need that in the Primary Key
We also need a composite key to make that unique, so I've added in userid
We want the query to return firstName and lastName, so these become our 3rd and 4th columns
"""
query = """CREATE TABLE IF NOT EXISTS song_listeners(
    song text,
    userId int,
    firstName text,
    lastName text,
    PRIMARY KEY(song, userId))"""

try:
    session.execute(query)
except Exception as e:
    print(e)

# Opens the previously created data file    
file = 'event_datafile_new.csv'
df = pd.read_csv(file)

# Query for inserting data to table
query = "INSERT INTO song_listeners(song, userId, firstName, lastName)"
query = query + " VALUES (%s, %s, %s, %s)"

# Loops through the DataFrame and inserts each row into the table
for i,row in df.iterrows():
    session.execute(query, (row['song'], row['userId'], row['firstName'], row['lastName']))



# In[ ]:


try:
    rows = session.execute(query3)
except Exception as e:
    print(e)
    
for row in rows:
    print (row)
                    


# ### Drop the tables before closing out the sessions

# In[ ]:


#Drop the table before closing out the sessions
tables = ['song_in_session','songs_in_user_session','song_listeners']
for table in tables:
    query = 'DROP TABLE ' + table
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)


# In[ ]:





# ### Close the session and cluster connection¶

# In[ ]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




