import requests
import pandas as pd
import datetime
import json
from sqlalchemy import create_engine
import psycopg2



def run_spotify_etl():
    
    DATABASE_LOCATION = 'postgresql+psycopg2://username:password@host:port/dbname'
    USER_ID = ''
    TOKEN = ''


 
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
       
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000


    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    artist_name = []
    song_name = []
    album_name = []
    played_at = []
    date = []

    for songs in data['items']:
        artist_name.append(songs['track']['artists'][0]['name'])
        song_name.append(songs['track']['name'])
        album_name.append(songs['track']['album']['name'])
        played_at.append(songs['played_at'])
        date.append(songs['played_at'][0:10])

    song_df = pd.DataFrame(
        {'song_name':song_name,
         'artist_name': artist_name,
         'album_name':album_name,
         'played_at' :played_at,
         'date':date},
         columns= ['song_name','artist_name','album_name','played_at','date'])

    

    engine = create_engine(DATABASE_LOCATION)
    conn = engine.raw_connection()
    cursor = conn.cursor()


    query = '''
    CREATE TABLE songs (
        song_name TEXT,
        artist_name TEXT,
        album_name TEXT,
        played_at TEXT,
        date DATE,
        constraint played_at_pkey PRIMARY KEY (played_at)
    )
    '''

    cursor.execute(query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")