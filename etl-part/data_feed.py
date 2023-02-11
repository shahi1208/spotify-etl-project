import spotipy
from spotipy.oauth2 import SpotifyOAuth
from flask import Flask 
import pandas as pd
import psycopg2
from  sqlalchemy import create_engine
import logging
from data_validation import check_valid_data

#creating app to get a redirect uri

# App config
app = Flask(__name__)


app.secret_key = 'something-random'
app.config['SESSION_COOKIE_NAME'] = 'spotify-login-cookie'

@app.route('/')
def login():
        return 'done'

logging.basicConfig(filename='data.log',filemode='w',level=logging.INFO)


if __name__ == '__main__':

    try:

        client_id='client_id'
        client_secret='client_secret'
        redirect_uri=  'http://localhost:5000/'

        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                    client_secret= client_secret,
                                                    redirect_uri=redirect_uri,
                                                    scope="user-read-recently-played"))

        data = sp.current_user_recently_played()
        logging.info('retrived data succesfully')
    except:
        logging.error('trouble in pulling the data',exc_info=True)


    try:
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
        
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_ts = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
        song_df['date'] = pd.to_datetime(song_df['date'])

        final_df = song_df[~(song_df['date'] != yesterday_ts)]
        print(final_df)
 
        logging.info('sucessfully transformed data')
    except:
        logging.error('error in formatting the data',exc_info=True)

    try:
        database = 'postgresql+psycopg2://username:password@host:port/dbname'

        engine = create_engine(database)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        logging.info('database setup done')
    except:
        logging.error('trouble in connecting the database',exc_info=True)


    try:

        if check_valid_data(final_df):

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

            final_df.to_sql('songs_table', engine, index=False, if_exists='append')
            conn.close()
            logging.info('uploaded to database')
    except:
        logging.error('data already exists',exc_info=True)

