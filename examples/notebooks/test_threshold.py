import pandas as pd
import numpy as np

def test_threshold(df_meeting,sampleDelay = 50,avg_speech_power_threshold = 42,custom_power_threshold=None):
    frame_size = 1000 #milliseconds
    median_window = 2*60*1000 #milliseconds
    median_window = int(median_window/sampleDelay)
    power_window = int(frame_size/sampleDelay)
    clipping_value = 120 #Maximum value of volume above which the signal is assumed to have non-speech external noise
    df_meeting = df_meeting.clip(upper=clipping_value)
    #Calculate the rolling median and subtract this value from the volume to remove the envelope
    df_median = df_meeting.apply(lambda x:x.rolling(min_periods=1,window=median_window,center=False).median())
    #df_median.plot(kind='area',subplots=True);plt.show()
    df_normalized = df_meeting - df_median
    #df_normalized[df_normalized>10]['PW5UJDRM35'].plot(subplots=True,kind='kde')
    #Calculate power and apply avg speech power threshold
    df_energy = df_normalized.apply(np.square)
    df_power = df_energy.apply(lambda x:x.rolling(window=power_window, min_periods=1,center=False).mean())
    power_mean = df_power.mean()
    power_pooled_mean = df_power.mean().mean()
    power_variance = df_power.var()
    
    power_threshold_single = power_mean + 2*np.sqrt(power_variance) # Outlier threshold mean +2*std

    power_pooled_variance = (power_variance+np.square(power_mean)).mean()-np.square(power_pooled_mean)
    power_threshold_pooled = power_pooled_mean + 2*np.sqrt(power_pooled_variance)
    
    df_is_speech = df_power > avg_speech_power_threshold
    #df_is_speech.plot(kind='area',subplots=True);plt.show()
    df_max_power = df_power.apply(np.max,axis=1)
    df_is_winner = df_power.apply(lambda x:x==df_max_power) #Find the badge with the highest power reading at every sample interval and declare it to be the main speaker
    
    #The assumption here is that there is only one speaker at any given time
    df_is_speech = df_is_speech & df_is_winner
    
    #fig, ax = plt.subplots()
    #df_meeting.plot(subplots=True,sharey=True)
    #df_power.plot(subplots=True)
    if(custom_power_threshold is None):
        #(df_power>power_threshold_single).plot(subplots=True)
        print 'power_variance : ',power_variance
        print 'power_mean : ',power_mean
        print 'power_threshold = (power_mean + 2*(power_variance)^0.5): ',power_threshold_single
        return power_threshold_single
    else:
        #(df_power>custom_power_threshold).plot(subplots=True)
        df_custom_power = df_power[df_power>custom_power_threshold]
        custom_power_mean = df_custom_power.mean()
        custom_power_variance = df_custom_power.var()
        custom_power_threshold_single = custom_power_mean + 2*np.sqrt(custom_power_variance)
        
        custom_power_pooled_mean = custom_power_mean.mean()
        custom_power_pooled_variance = (custom_power_variance+np.square(custom_power_mean)).mean()-np.square(custom_power_pooled_mean)
        custom_power_threshold_pooled = custom_power_pooled_mean + 2*np.sqrt(custom_power_pooled_variance)
        print 'For all power samples greater than custom_power_threshold = ', custom_power_threshold
        print 'power_mean : ', custom_power_mean
        print 'power_variance : ', custom_power_variance
        print 'power_threshold = (power_mean + 2*(power_variance)^0.5): ', custom_power_threshold_single
        return custom_power_threshold_single
    #df_power.hist(bins = 40)
    #df_power_mean.plot(subplots=True,sharey=True)
    #(df_power_mean+df_power_std).plot(subplots=True,sharey=True)

def is_speaking(df_meeting,avg_speech_power_threshold = 42,sampleDelay = 50):
    frame_size = 1000 #milliseconds
    median_window = 2*60*1000 #milliseconds
    median_window = int(median_window/sampleDelay)
    power_window = int(frame_size/sampleDelay)
    clipping_value = 120 #Maximum value of volume above which the signal is assumed to have non-speech external noise
    df_meeting = df_meeting.clip(upper=clipping_value)
    #Calculate the rolling median and subtract this value from the volume to remove the envelope
    df_median = df_meeting.apply(lambda x:x.rolling(min_periods=1,window=median_window,center=False).median())
    #df_median.plot(kind='area',subplots=True);plt.show()
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

def fill_boolean_segments(x_series,min_length,value):
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



def get_stitched(df_is_speech,min_talk_length=2000,min_gap_size=500,sampleDelay = 50):
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

def get_speaking_stats(df_meeting,sampleDelay = 50):
    #This function uses the data from a meeting to return 
    ####a.the number of turns per speaker per minute
    ####b.the total speaking time
    #Use speaking/not speaking function
    #Use stitching function
    #Expected input: A dataframe with a datetime index and one column per badge. 
    #Each column contains a time-series of absolute value speech volume samples
    df_is_speech = is_speaking(df_meeting)
    df_is_speech.plot(kind='area',subplots=True);plt.show()
    df_stitched = get_stitched(df_is_speech)
    df_stitched.plot(kind='area',subplots=True);plt.show()
    all_stats=[]
    for member in df_stitched.columns.values:
        current_member = {}
        current_member['member'] = member
        current_member['totalTurns'] = len([ sum( 1 for _ in group ) for key, group in itertools.groupby(df_stitched[member]) if key ])
        print sum(df_stitched[member])*sampleDelay
        current_member['totalSpeakingTime'] = datetime.timedelta(milliseconds=sum(df_stitched[member])*sampleDelay) #if len(all_segments)>0 else datetime.timedelta(0)
        #current_member['total_speaking_time'] = np.sum(all_segments['length'])*sampleDelay
        all_stats.append(current_member)
    return all_stats

def get_speaking_series(df_meeting,sampleDelay = 50):
    def custom_resampler(array_like):
        return len([ sum( 1 for _ in group ) for key, group in itertools.groupby(array_like) if key ])

    df_is_speech = is_speaking(df_meeting)
    #df_is_speech.plot(kind='area',subplots=True);plt.show()
    df_stitched = get_stitched(df_is_speech)
    #df_stitched.plot(kind='area',subplots=True);plt.show()
    df_stitched = df_stitched.resample('1T').apply(custom_resampler)

    return df_stitched