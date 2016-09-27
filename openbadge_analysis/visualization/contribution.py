import datetime

from bokeh.charts import Area, output_file, show
from bokeh.models.formatters import DatetimeTickFormatter
from bokeh.models.widgets import Panel, Tabs


def unix_time_ms(dt):
    """
    Converts datetime to timestamp float (milliseconds) for plotting
    :param dt: datetime
    :return: timestamp float (ms)
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds()*1000


def contribution_plot(df_stitched, meeting_name, rolling=True, member_names=None):
    """
    Creates a collection of 4 stacked area graphs that show seconds of contribution per member per minute for a meeting.
    The four graphs are: 1 min, 30 sec, 10 sec, 5 sec resampling frequencies.
    :param df_stitched: DataFrame whose values are boolean and indicate whether a badge wearer (column name) was
    speaking at a particular timestamp, has columns: datetime, member1, member2, etc.
    :param meeting_name: Name of meeting (usually uuid), i.e. the part of the log file before '.txt'
    :param rolling: True or False. Whether or not to generate the graph with a rolling mean (which makes the graph
    smoother but might not most accurately represent the data). True by default
    :param member_names: A dictionary mapping member keys to member names (First Last format)
    :return: bokeh Tabs holding 4 stacked area graphs
    """
    def area_chart(df, interval, rolling):
        # re-sampling
        df = df_stitched.resample(str(interval)+'S').sum().fillna(0)/(1000/50)*(60/interval)  # Each sample is 50ms
        # Gives number of seconds spoken per min

        # rename columns if names were given
        if member_names:
            for member_key in member_names:
                df.rename(columns=member_names, inplace=True)

        if rolling:
            df = df.rolling(min_periods=1, window=5, center=True).mean()  # To smooth graph

        start = unix_time_ms(df.index[0])
        start_datetime = datetime.datetime.utcfromtimestamp(start/1000)
        end = unix_time_ms(df.index[len(df.index)-1])
        end_datetime = datetime.datetime.utcfromtimestamp(end/1000)
        
        df.reset_index(level='datetime', inplace=True)  # To input x values into area chart

        if rolling:
            graph_title = 'Contribution per Minute per Member for Meeting ' + meeting_name + ' (with rolling mean) \
            from ' + start_datetime.strftime('%I:%M %p')+' to '+end_datetime.strftime('%I:%M %p')
        else:
            graph_title = 'Contribution per Minute per Member for Meeting ' + meeting_name + ' (without rolling mean) \
            from ' + start_datetime.strftime('%I:%M %p')+' to '+end_datetime.strftime('%I:%M %p')
        
        area = Area(        
            df,
            x='datetime',  # Column name
            title=graph_title, legend='top_left',
            stack=True, xlabel='Time of Day', ylabel='Number of Seconds',
            xscale='datetime',
            width=1700, height=400,          
            tools='xpan, xwheel_zoom, box_zoom, reset, resize',
        )

        # Format tick labels on x-axis
        area.below[0].formatter = DatetimeTickFormatter()
        area.below[0].formatter.formats = dict(years=['%Y'], months=['%b %Y'], days=['%d %b %Y'],
                                               hours=['%I:%M %P'], hourmin=['%I:%M %P'],
                                               minutes=['%I:%M %P'], minsec=['%I:%M:%S %P'],
                                               seconds=['%I:%M:%S %P']) 
        
        return area
    
    area5 = area_chart(df_stitched, 5, rolling)
    tab5 = Panel(child=area5, title='5 Second Intervals')
    
    area10 = area_chart(df_stitched, 10, rolling)
    tab10 = Panel(child=area10, title='10 Second Intervals')
    
    area30 = area_chart(df_stitched, 30, rolling)
    tab30 = Panel(child=area30, title='30 Second Intervals')
    
    area60 = area_chart(df_stitched, 60, rolling)
    tab60 = Panel(child=area60, title='60 Second Intervals')
    
    plots = Tabs(tabs=[tab60, tab30, tab10, tab5])
    return plots