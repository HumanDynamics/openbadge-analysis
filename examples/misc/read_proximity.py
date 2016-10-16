import json
import pandas as pd
import numpy as np
import itertools
import datetime
import operator

def proximity2data(input_file_path, datetime_index=True, resample=True, log_version=None):

    with open(input_file_path,'r') as input_file:
        raw_data = input_file.readlines() #This is a list of strings
        #meeting_metadata = json.loads(raw_data[0]) #Convert the header string into a json object
        print(len(raw_data ))
        batched_proximity_data = []
        for row in raw_data[0:]:
            data = json.loads(row)
            if data['type'] == 'proximity received':
                batched_proximity_data.append(data['data'])

        proximity_data = []
        for j in range(len(batched_proximity_data)):
            batch = {}
            batch.update(batched_proximity_data[j])  # Create a deep copy of the jth batch of samples

            sample = {'badge_address': batch.pop('badge_address'), 'timestamp': batch.pop('timestamp'), 'member':batch.pop('member')}
            proximity_data.append(sample)

    if len(proximity_data) == 0:
        return None

    df_proximity_data = pd.DataFrame(proximity_data)

    # TODO: might have a problem converting to UTC or local time here? original timestamp is ms
    df_proximity_data['datetime'] = pd.to_datetime(df_proximity_data['timestamp'], unit='s')
    # TODO: Is this needed? Does indexing by datetime already does that?
    df_proximity_data.sort_values('datetime')


    df_proximity_data.set_index(pd.DatetimeIndex(df_proximity_data['datetime']), inplace=True)
    del df_proximity_data['timestamp']
    df_proximity_data.index.name = 'datetime'
    del df_proximity_data['datetime']

    return df_proximity_data

if __name__ == "__main__":
    a = proximity2data("../../../../Downloads/pi10_log_proximity.txt")
    print a

'''
        batched_sample_data = []
        for row in raw_data[1:]:
            data = json.loads(row)
            if data['type'] == 'audio received':
                batched_sample_data.append(data['data'])

    else:
        raise Exception('file log version was not set and cannot be identified')

    sample_data = []

    for j in range(len(batched_sample_data)):
        batch = {}
        batch.update(batched_sample_data[j]) #Create a deep copy of the jth batch of samples
        samples = batch.pop('samples')
        if log_version == '1.0':
            reference_timestamp = batch.pop('timestamp')*1000+batch.pop('timestamp_ms') #reference timestamp in milliseconds
            sampleDelay = batch.pop('sampleDelay')
        elif log_version == '2.0':
            reference_timestamp = batch.pop('timestamp')*1000 #reference timestamp in milliseconds
            sampleDelay = batch.pop('sample_period')
        numSamples = len(samples)
        #numSamples = batch.pop('numSamples')
        for i in range(numSamples):
            sample = {}
            sample.update(batch)
            sample['signal'] = samples[i]

            sample['timestamp'] = reference_timestamp + i*sampleDelay
            sample_data.append(sample)

    df_sample_data = pd.DataFrame(sample_data)
    if len(sample_data)==0:
        return None
    df_sample_data['datetime'] = pd.to_datetime(df_sample_data['timestamp'], unit='ms')
    df_sample_data['datetime'] = df_sample_data['datetime'] - np.timedelta64(4, 'h')
    del df_sample_data['timestamp']

    df_sample_data.sort_values('datetime')

    if(datetime_index):
        df_sample_data.set_index(pd.DatetimeIndex(df_sample_data['datetime']),inplace=True)
        #The timestamps are in UTC. Convert these to EST
        #df_sample_data.index = df_sample_data.index.tz_localize('utc').tz_convert('US/Eastern')
        df_sample_data.index.name = 'datetime'
        del df_sample_data['datetime']
        if(resample):
            grouped = df_sample_data.groupby('member')
            df_resampled = grouped.resample(rule=str(sampleDelay)+"L").mean()

    if(resample):
        # Optional: Add the meeting metadata to the dataframe
        df_resampled.metadata = meeting_metadata
        return df_resampled
    else:
        # Optional: Add the meeting metadata to the dataframe
        df_sample_data.metadata = meeting_metadata
        return df_sample_data


def is_speaking(df_meeting, sampleDelay = 50):
    frame_size = 1000 #milliseconds
    median_window = 2*60*1000 #milliseconds
    median_window = int(median_window/sampleDelay)
    power_window = int(frame_size/sampleDelay)
    clipping_value = 120 #Maximum value of volume above which the signal is assumed to have non-speech external noise
    df_meeting = df_meeting.clip(upper=clipping_value)
    avg_speech_power_threshold = 42
    #Calculate the rolling median and subtract this value from the volume
    df_median = df_meeting.apply(lambda x:x.rolling(min_periods=1,window=median_window,center=False).median())
    df_normalized = df_meeting - df_median
    #Calculate power and apply avg speech power threshold
    df_energy = df_normalized.apply(np.square)
    df_power = df_energy.apply(lambda x:x.rolling(window=power_window, min_periods=1,center=False).mean())
    df_is_speech = df_power > avg_speech_power_threshold
    #df_is_speech.plot(kind='area',subplots=True);plt.show()
    df_max_power = df_power.apply(np.max,axis=1)
    df_is_winner = df_power.apply(lambda x:x==df_max_power) #Find the badge with the highest power reading at every sample interval and declare it to be the main speaker
    #The assumption here is that there is only one speaker at any given time

    df_is_speech = df_is_speech & df_is_winner
    return df_is_speech


def fill_boolean_segments(x_series, min_length, value):
    #Given a boolean series fill in all value (True or False) sequences less than length min_length with their inverse
    total_samples = len(x_series)
    not_value = not value
    i=0
    length=0
    start=0
    while(i<total_samples):
        current_value = x_series[i]
        if(i==0):
            previous_value = current_value

        if((previous_value != current_value) or (i==total_samples-1)):
            stop = i
            if(length<min_length and previous_value==value):
                x_series[start:stop] = not_value
            length=1
            start=i
        else:
            length+=1
        i=i+1
        previous_value = current_value


def make_stitched(df_is_speech, min_talk_length=2000, min_gap_size=500, sampleDelay = 50):
    min_talk_length_samples = int(min_talk_length/sampleDelay)
    min_gap_size_samples = int(min_gap_size/sampleDelay)
    df_is_gap = df_is_speech.copy()

    for member in df_is_speech.columns.values:
        #First fill all the gaps less than min_gap_size (milliseconds)
        #Set the corresponding samples to True in df_is_gap
        fill_boolean_segments(df_is_gap[member],min_gap_size_samples,False)
        #Then find all the True segments which are less than min_talk_length (milliseconds) and invert them
        fill_boolean_segments(df_is_gap[member],min_talk_length_samples,True)

    return df_is_gap


#takes in df from sample2data
def make_df_stitched(df_meeting):
    if df_meeting is not None:
        #df_meeting = pd.pivot_table(df_meeting.reset_index(), index="datetime", columns = "member", values = "signal").dropna()
        df_meeting = pd.pivot_table(df_meeting.reset_index(), index="datetime", columns="member",
                                    values="signal").fillna(False)

        #Expected input: A dataframe with a datetime index and one column per badge.
        df_is_speech = is_speaking(df_meeting)
        df_stitched = make_stitched(df_is_speech)

        return df_stitched
    else:
        return "No meeting data"


#takes in df from make_df_stitched
def get_turns(df_stitched, sampleDelay=50):
    all_stats=[]
    for member in df_stitched.columns.values:
        current_member = {}
        #current_member['date'] = df_stitched.index[0].strftime('%Y-%m-%d')
        current_member['member'] = member
        #print([(key, list(group)) for key, group in itertools.groupby(df_stitched[member])])
        current_member['totalTurns'] = len([ sum( 1 for _ in group ) for key, group in itertools.groupby(df_stitched[member]) if key ])  # includes self-turns
        # If NaN values for a member, e.g. when creating values for multiple meetings, but a member wasn't in one of the
        # meetings, fill NaN with 0
        df_stitched.fillna(0, inplace=True)
        current_member['totalSpeakingTime'] = datetime.timedelta(milliseconds=sum(df_stitched[member])*sampleDelay).total_seconds()
        all_stats.append(current_member)
    return all_stats


def total_turns(df_stitched):
    df_turns = pd.DataFrame()
    members_stats = get_turns(df_stitched)
    df_turns = df_turns.append(pd.DataFrame(members_stats))
    duration = (df_stitched.index[-1] - df_stitched.index[0]).total_seconds()
    df_turns.duration = duration
    #columns: member, totalSpeakingTime, totalTurns for each member
    return df_turns
'''