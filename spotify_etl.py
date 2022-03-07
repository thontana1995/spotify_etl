import curses
import sqlalchemy 
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_player_sqlite"
USER_ID =  "b."
TOKEN = "BQDdbnAblG-1fKISx5l09zIOCNVO4Tm-uwRiXS3tloAghvwpjzjHKoGif_xT098yHDW9r5t6E8R8ZF2stBVvs7fGTwoB2Zu-x8tM4br1DBOAKSniJCV9Z0cWHOzJYRlP7LUQwHWv7qs7AtdFY89XaCqK5TAIMeWqjUgTKVNsFw3v"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No song downloaded, Finised execution")
        return False
    
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")
    
    if df.isnull().values.any():
        raise Exception("Null values found")
    
    yesterday = datetime.datetime.now() - datetime.timestamp(day = 1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%M-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    
    return True
    


if __name__ == "__main__":

    header = {
        "Accept" : "application/json",
        "content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(day=1) 
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp, header = headers))

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["item"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"])

    song_dict = {
        "song_name" : song_names,
        "artist_names" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, column = ["song_names", "artist_names", "played_at", "timestamp"])
    
    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, process to Load stage")
    
    #Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("my_played_track.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRANT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(sql_query)
    print("Opened database successfully")
    
    try:
        song_df.to_sql("my_played_tracts", engine, index=False, if_exists="append")
    except:
        print("Data already exists in Database")

    conn.close()
    print("close database successfully")
