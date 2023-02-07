import spotipy
from spotipy.oauth2 import SpotifyOAuth
from flask import Flask 
import pandas as pd
import psycopg2
from  sqlalchemy import create_engine

# App config
app = Flask(__name__)


app.secret_key = 'smth-randm'
app.config['SESSION_COOKIE_NAME'] = 'spotify-login-cookie'

@app.route('/')
def login():
        return 'done'

logging.basicConfig(filename='data.log',filemode='w',level=logging.INFO)


def spotify():

 
        client_id='client_id'
        client_secret='client_secret'
        redirect_uri=  'http://localhost:5000'

        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                    client_secret= client_secret,
                                                    redirect_uri=redirect_uri,
                                                    scope="user-read-recently-played"))

        data = sp.current_user_recently_played()

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

        database = 'postgresql+psycopg2://usr:pw@host:port/dbname'

        engine = create_engine(database)
        conn = engine.raw_connection()
        cursor = conn.cursor()


        def check_valid_data(song_df):
            if df.empty:
                print('no songs were listened yesterday')
            return False

            if pd.Series(df['played_at']).unique:
                pass
            else:
                raise Exception ('primary key is violated')

            if df.isnull().values.any():
                raise Exception('null values found')


            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            yesterday_ts = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
            timestamps = df['date'].to_list()
            for timestamp in timestamps:
                if datetime.datetime.strptime(timestamp,'%Y-%m-%d') != yesterday_ts:
                    raise Exception('invalid data, song is not from yesterday')
                else:
                    pass

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

            song_df.to_sql('songs_table', engine, index=False, if_exists='append')
            conn.close()
