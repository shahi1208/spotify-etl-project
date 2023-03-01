# Weekly Top 5 Songs



A Python wrapper for the Spotify API using the Spotipy library to extract data,perform data manipulation, and upload it to a database. 

The wrapper is automated using Apache Airflow to only extract data from the previous day to avoid duplicates and ensure that the data is up-to-date.

Using the data loaded into the database, the script also generates a weekly top 5 songs chart. The chart is based on the number of plays in the previous week and is updated every time the Airflow DAG is run. The top 5 songs can be accessed in the database.

# Requirements
create a virtual environment

Spotipy (pip install spotipy) and required packages 

Access to the Spotify API (you can get started with [spotipy api](https://github.com/spotipy-dev/spotipy))

Apache Airflow (instructions on how to install can be found [airflow official docs](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html))

# Usage

Clone this repository to your local machine

Replace <YOUR_SPOTIFY_CLIENT_ID> and <YOUR_SPOTIFY_CLIENT_SECRET> in data_feed.py with your Spotify API client ID and secret

Set up your database connection in data_feed.py.py

Set up Apache Airflow to run the data_feed DAG (instructions on how to do this can be found [airflow dag docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html))

Run the DAG in Apache Airflow to extract and load data from the Spotify API
# Data Validation
The script performs basic data validation to ensure that the data uploaded to the database is correct. In case of any errors, an exception is raised with a descriptive error message.
