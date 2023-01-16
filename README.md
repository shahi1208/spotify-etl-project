# spotify-etl-project

This project is about the recently played trackers from the spotify app. 

DATAFEED:



    created a dataframe using the json data from api for future analytics purpose.
    created a table in postgres and connected the script with psycopy2 
    refreshing the token and running the script will load the new data into the database.
    
DATA VALIDATION :

    - the validated the data so there is no duplicate data present in the table.
    
to make the workflow automated i used airflow, the dag code and edited data feed code is in the dag folder.
