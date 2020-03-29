import pandas as pd
import os
from datetime import datetime

#maximum session duration in minutes
MAX_SESSION_DURATION = 1
id_counter = 0

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
    
    # dict = build_dictionary(df)
    return df

def clean_edit_events():
    df = import_data('../data/','editEvents.csv')
    df['timestamp'] = df['timestamp'].apply(parse_timestamp)
    return df

    

def clean_test_events():
    df = import_data('../data/','testEvents.csv')
    df['timestamp'] = df['timestamp'].apply(parse_timestamp)

    df['totalTests'] = df['totalTests'].apply(to_int)
    df['testsPassed'] = df['testsPassed'].apply(to_int)
    return df

    
def build_dictionary(df):
    dict = {}
    for index, row in df.iterrows():
        #check if session id is registered
        if row['sessionID'] in dict:
            dict[row['sessionID']].append(row[1:])
        else:
            dict[row['sessionID']] = []
    return dict

def generate_id():
    global id_counter
    old_id = id_counter
    id_counter +=1
    return old_id

def select_and_merge(df1, df2, df3):
    return pd.concat([df1[['sessionID', 'timestamp']], df2[['sessionID', 'timestamp']], df3[['sessionID', 'timestamp']]])

def update_id(dfs, id, timestamp, new_id):
    for df in dfs:
        print(f'updating df')
        print(f'row: {df.loc[(df["sessionID"] == id) & (df["timestamp"] == timestamp), "sessionID"]}' )
        df.loc[(df['sessionID'] == id) & (df['timestamp'] == timestamp), 'sessionID'] = new_id
        print(f'row: {df.loc[(df["sessionID"] == new_id) & (df["timestamp"] == timestamp), "sessionID"]}' )


    return (dfs[0], dfs[1], dfs[2])

# todo: figure out splitting logic
def split_long_sessions(df1, df2, df3):
    df = select_and_merge(df1, df2, df3)
    df = df.sort_values(by=['sessionID', 'timestamp'])
    print(df.head())
    # initial id of each session
    initial_id = df['sessionID'].iloc(0)
    # start of each session
    session_start = df['timestamp'].iloc(0)
    new_id = None
    is_split = False
    test_index = 0
    for index, row in df.iterrows():
        if test_index > 10:
            return (df1, df2, df3)
        test_index += 1

        # true as long as we are considering rows that originate from the same session
        if row['sessionID'] == initial_id:
            # check if we have previously split up this session
            if is_split:
                df1, df2, df3 = update_id([df1, df2, df3], row['sessionID'], row['timestamp'], new_id)
                row['sessionID'] = new_id
                df.loc[index, 'sessionID'] = new_id
                
            difference = (row['timestamp'] - session_start).seconds / 60
            if difference >= MAX_SESSION_DURATION:
                
                is_split = True
                new_id = generate_id()
                df1, df2, df3 = update_id([df1, df2, df3], row['sessionID'], row['timestamp'], new_id)
                df.loc[index, 'sessionID'] = new_id

                session_start = row['timestamp']
        # completely new session
        else:
            initial_id = row['sessionID']
            session_start = row['timestamp']
            new_id = None
            is_split = False
    
    return (df1, df2, df3)





def import_data(path, filename):
    return pd.read_csv(os.path.join(path, filename))
    




if __name__ == "__main__":
    df_build = clean_build_events()
    df_edit = clean_edit_events()
    df_test = clean_test_events()
    # print(df.head(10))
    df_build, df_edit, df_test = split_long_sessions(df_build, df_edit, df_test)
    df_build = df_build.sort_values(by=['sessionID', 'timestamp'])
    df_edit = df_edit.sort_values(by=['sessionID', 'timestamp'])
    df_test = df_test.sort_values(by=['sessionID', 'timestamp'])

    # test = split_long_sessions(df_build, df_edit, df_test)
    # print(test)
    # print(df.head(10))
    print(df_build.head())
    print(df_edit.head())
    print(df_test.head())

