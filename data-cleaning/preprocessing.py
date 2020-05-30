import pandas as pd
import os
from datetime import datetime
import numpy as np

#maximum session duration in minutes
MAX_SESSION_DURATION = 60
SESSION_TIMEOUT_SECONDS = 300 # after that many seconds of no activity the session counts as ended
# SESSION_TIMEOUT_SECONDS = 1
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
    df['type'] = 'build'
    # dict = build_dictionary(df)
    return df

def clean_edit_events():
    df = import_data('../data/','editEvents.csv')
    df['timestamp'] = df['timestamp'].apply(parse_timestamp)
    df['type'] = 'edit'

    return df

    

def clean_test_events():
    df = import_data('../data/','testEvents.csv')
    df['timestamp'] = df['timestamp'].apply(parse_timestamp)

    df['totalTests'] = df['totalTests'].apply(to_int)
    df['testsPassed'] = df['testsPassed'].apply(to_int)
    df['type'] = 'test'

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
    return pd.concat([df1[['sessionID', 'timestamp', 'type', 'buildSuccessful']], df2[['sessionID', 'timestamp', 'type']], df3[['sessionID', 'timestamp', 'type', 'totalTests', 'testsPassed']]])

def update_id(dfs, id, timestamp, new_id):
    for df in dfs:
        #print(f'updating df')
        #print(f'row: {df.loc[(df["sessionID"] == id) & (df["timestamp"] == timestamp), "sessionID"]}' )
        df.loc[(df['sessionID'] == id) & (df['timestamp'] == timestamp), 'sessionID'] = new_id
        #print(f'row: {df.loc[(df["sessionID"] == new_id) & (df["timestamp"] == timestamp), "sessionID"]}' )


    return (dfs[0], dfs[1], dfs[2])

def update_id_np(arrays, id, timestamp, new_id):
    for a in arrays:
        ind = np.where((a[:,0] == id) & (a[:,1] == timestamp))
        a[ind, 0] = new_id

# todo: figure out splitting logic
def split_long_sessions(df1, df2, df3):
    df = select_and_merge(df1, df2, df3)
    df = df.sort_values(by=['sessionID', 'timestamp'])
    # print(df.head())

    new_id = None
    is_split = False
    
    array = df.to_numpy()
    array_1 = df1.to_numpy()
    array_2 = df2.to_numpy()
    array_3 = df3.to_numpy()
    # initial id of each session
    initial_id = array[0,0] # df['sessionID'].iloc(0)
    # start of each session
    session_start = array[0,1] # df['timestamp'].iloc(0)
    last_activity = session_start
    print(array[0])
    print(array.shape)
    total_rows = array.shape[0]
    print(f'splitting sessions for {total_rows} rows')

    for index in range(0, total_rows):
        #print(f'iteration: {index}')
        #print(f'session id: {array[index, 0]}')
        if index % int(total_rows/100) == 0:
            print(f'-- {int(index/total_rows*100)}%') 
        # true as long as we are considering rows that originate from the same session
        if array[index,0] == initial_id:
            # check if we have previously split up this session
            if is_split:
                # update_id_np([array_1, array_2, array_3], array[index, 0], array[index, 1], new_id)
                array[index,0] = new_id
                # df.loc[index, 'sessionID'] = new_id
            #print(f'session start: {session_start}')
            #print(f'comparing against: {array[index, 1]}')
            session_delta = array[index, 1] - session_start
            session_duration = session_delta.seconds/60
            #print(f'duration: {session_duration}')
            time_since_last_activity = (array[index, 1] - last_activity).seconds
            #print(f'time since last edit: {time_since_last_activity}')
            if session_duration > MAX_SESSION_DURATION or time_since_last_activity > SESSION_TIMEOUT_SECONDS:
                
                is_split = True
                new_id = generate_id()
                # update_id_np([array_1, array_2, array_3], array[index, 0], array[index, 1], new_id)
                array[index, 0] = new_id

                session_start = array[index, 1]
            last_activity = array[index, 1]
        # completely new session
        else:
            initial_id = array[index, 0]
            session_start = array[index, 1]
            last_activity = session_start
            new_id = None
            is_split = False
    
    return array





def import_data(path, filename):
    return pd.read_csv(os.path.join(path, filename))
    

def save_to_file(df, filename):
    df.to_csv(filename, encoding='utf-8')


if __name__ == "__main__":
    df_build = clean_build_events()
    df_edit = clean_edit_events()
    df_test = clean_test_events()
    # print(df.head(10))
    array = split_long_sessions(df_build, df_edit, df_test)
    # get indexes for each type
    build_events = np.where((array[:,2] == 'build'))
    edit_events = np.where((array[:,2] == 'edit'))
    test_events = np.where((array[:,2] == 'test'))
    # get the data, drop not needed columns
    df_build = pd.DataFrame(array[build_events])
    df_build.columns = ['sessionID', 'timestamp', 'type', 'buildSuccessful', 'totalTests', 'testsPassed']
    df_build = df_build.drop(['totalTests', 'testsPassed'], axis=1)
    df_edit = pd.DataFrame(array[edit_events])
    df_edit.columns = ['sessionID', 'timestamp', 'type', 'buildSuccessful', 'totalTests', 'testsPassed']
    df_edit = df_edit.drop(['buildSuccessful', 'totalTests', 'testsPassed'], axis=1)
    df_test = pd.DataFrame(array[test_events])
    df_test.columns = ['sessionID', 'timestamp', 'type', 'buildSuccessful', 'totalTests', 'testsPassed']
    df_test = df_test.drop('buildSuccessful', axis=1)
    # sort
    df_build = df_build.sort_values(by=['sessionID', 'timestamp'])
    df_edit = df_edit.sort_values(by=['sessionID', 'timestamp'])
    df_test = df_test.sort_values(by=['sessionID', 'timestamp'])

    # test = split_long_sessions(df_build, df_edit, df_test)
    # print(test)
    # print(df.head(10))
    print(df_build.head())
    print(df_edit.head())
    print(df_test.head())

    save_to_file(df_build, 'df_build.csv')
    save_to_file(df_edit, 'df_edit.csv')
    save_to_file(df_test, 'df_test.csv')

