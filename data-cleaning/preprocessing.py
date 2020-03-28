import pandas as pd
import os
from datetime import datetime

# parsing timestamps into python timestamps
def parse_timestamp(time):
    if type(time) is str:
        time = time.split('.')[0]
        return pd.Timestamp(time)

# Converting test passed/total to integer as it has to be a integer value.
# nan values are converted to -1, this is subject to change (we might just delete them?)
def to_int(value):
    try:
        return int(value)
    except:
        return -1

def clean_build_events():
    df = import_data('../data/','buildEvents.csv')
    df['timestamp'] = df['timestamp'].apply(parse_timestamp)
    print(df.head())
    dict = build_dictionary(df)

def clean_edit_events():
    df = import_data('../data/','editEvents.csv')
    df['timestamp'] = df['timestamp'].apply(parse_timestamp)
    print(df.head())

    

def clean_test_events():
    df = import_data('../data/','testEvents.csv')
    df['timestamp'] = df['timestamp'].apply(parse_timestamp)

    df['totalTests'] = df['totalTests'].apply(to_int)
    df['testsPassed'] = df['testsPassed'].apply(to_int)
    print(df.head())

    
def build_dictionary(df):
    dict = {}
    for index, row in df.iterrows():
        #check if session id is registered
        if row['sessionID'] in dict:
            dict[row['sessionID']].append(row[1:])
        else:
            dict[row['sessionID']] = []
    return dict

# todo: figure out splitting logic
def split_long_sesions(dict):

    


def split_long_sessions():
    pass


def import_data(path, filename):
    return pd.read_csv(os.path.join(path, filename))
    




if __name__ == "__main__":
    clean_build_events()
    clean_edit_events()
    clean_test_events()
