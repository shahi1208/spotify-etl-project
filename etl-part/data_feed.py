import requests
import pandas as pd
import datetime
import json
from sqlalchemy import create_engine
import psycopg2
from data_validation import check_valid_data


user_id = 'username'
token = 'generated request token'


if __name__ == "__main__":
    headers = { 
        "Accept" : "application/json" ,
        "Content-Type" : "application/json" ,
        "Authorization" : "Bearer {token}".format(token=token)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000


    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?limit=20&after={time}'.format(time= yesterday_unix_timestamp),headers=headers)
    data = r.json()

    # print(data['items'][0]['track']['artists'][0]['name']) # artist name
    # print(data['items'][0]['track']['name']) # song name
    # print(data['items'][0]['track']['album']['name']) # album name
    # print(data['items'][0]['played_at']) # played at
    # print(data['items'][0]['played_at']) # played at date

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

    
    if check_valid_data(song_df):
        print("data is ready to upload")

    database = 'postgresql+psycopg2://usrpw@host:port/dbname'

    engine = create_engine(database)
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

    try:
        song_df.to_sql('songs_table', engine, index=False, if_exists='append')
    except:
        print('database already exists')

    conn.close()

    
