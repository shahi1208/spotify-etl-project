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
        
        return True
