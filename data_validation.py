import pandas as pd
import datetime

def check_valid_data(df = pd.DataFrame) -> bool :

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
    timestamps = df['date']#.to_list()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp,'%Y-%m-%d') != yesterday_ts:
            raise Exception('invalid data, song is not from yesterday')
        
        return True