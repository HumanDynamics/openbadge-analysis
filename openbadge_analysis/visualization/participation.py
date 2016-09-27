import ast
import pandas as pd
import openbadge_analysis as ob

from bokeh.plotting import *
from bokeh.charts import Bar, output_file, show
from bokeh.charts.attributes import cat
from bokeh.models import HoverTool
from bokeh.models.tools import HoverTool, BoxZoomTool, ResetTool, PanTool, ResizeTool, WheelZoomTool
from bokeh.models.widgets import Panel, Tabs
from bokeh.io import output_file, show

def stack_bar(data_dict):
    """
    Creates a stacked bar graph showing percentages of participation for each member for each day/week (hover doesn't work)
    :param data_dict: Dictionary for stacked bar chart input. Maps each chart attribute to a list (with all lists being same length):
    {'labels': [Day1, Day2, ..., Day1, Day2, ...],
     'members': [Member1, Member1, ..., Member2, Member2, ...],
     'turns': [...],
     'speak': [...] }
    :return: bokeh plot
    """
    # Percentage of turns graph
    # Removed percentages from hover tool because didn't show correct percentages (y values accumulated)
    hover_turns = HoverTool(
        tooltips="""
        <div>
        <div>
        <span style="font-size: 17px; font-weight: bold;">@member</span>
        </div>
        <div>
        <span style="font-size: 17px;">@labels</span>
        </div>
        </div>
        """
    )
    # turns = Bar(data=data_dict, values='turns', label='week_num', stack='member',
    turns = Bar(data=data_dict, values='turns', label=cat(columns='labels', sort=False), stack='member',
                title='Member Participation by Speaking Turns', legend='top_right',
                plot_width=800,
                plot_height=600,
                tools=[hover_turns])
    # tools=[BoxZoomTool(), WheelZoomTool(), ResetTool()])
    label_font_style = 'normal'  # 'italic', 'bold'
    turns.below[0].axis_label = 'Dates'
    turns.below[0].major_label_orientation = 'horizontal'
    turns.below[0].axis_label_text_font_style = label_font_style
    turns.left[0].axis_label = 'Your Percentage of Participation (Based on Number of Speaking Turns)'
    turns.left[0].axis_label_text_font_style = label_font_style

    # Percentage of speaking time graph
    hover_speak = HoverTool(
        tooltips="""
        <div>
        <div>
        <span style="font-size: 17px; font-weight: bold;">@member</span>
        </div>
        <div>
        <span style="font-size: 17px;">@labels</span>
        </div>
        </div>
        """
    )
    speak = Bar(data=data_dict, values='speak', label=cat(columns='labels', sort=False), stack='member',
                title='Member Participation by Speaking Time', legend='top_right',
                plot_width=800,
                plot_height=600,
                tools=[hover_speak])
    # tools=[BoxZoomTool(), WheelZoomTool(), ResetTool()])
    speak.below[0].axis_label = 'Dates'
    speak.below[0].major_label_orientation = 'horizontal'
    speak.below[0].axis_label_text_font_style = label_font_style
    speak.left[0].axis_label = 'Your Percentage of Participation (Based on Amount of Speaking Time)'
    speak.left[0].axis_label_text_font_style = label_font_style

    tab_turns = Panel(child=turns, title='Participation by Speaking Turns')
    tab_speak = Panel(child=speak, title='Participation by Speaking Time')
    bars = Tabs(tabs=[tab_speak, tab_turns])
    return bars


# Process data for percentage participation and transitions for a group (uuids_all is a list of lists of meetings for each week)
def percentage_participation(df_stitched_all, labels, member_names=None):
    """
    Process data for percentage participation for a group
    :param df_stitched_all: a list of lists of df_stitched
    :param labels: a list of dates/weeks for which the df_stitched lists are for
    :param member_names: A dictionary mapping member keys to member names (First Last format)
    :return: participation_values Dict{ Member : { date : {'turns': float, 'speak, float } }, Member : ... }
    turns include self-turns
    """
    members_avg = {}
    total_turns_all = 0
    total_speaking_time_all = 0
    participation_values = {}

    for i in range(len(df_stitched_all)):
        label = labels[i]
        df_stitched_list = df_stitched_all[i]
        if len(df_stitched_list) == 0:
            print('No meetings for ' + str(label))
            continue
        df = pd.DataFrame()
        print('Generating data for  ' + str(label))
        for df_stitched in df_stitched_list:
            df_turns = ob.total_turns(df_stitched)
            df_turns.set_index('member', inplace=True)
            df = df.add(df_turns, fill_value=0)

        # For calculation of participation for avg weeks (for last bar in bargraph)
        for member in df.index.values:
            totalTurns = df.loc[member]['totalTurns']
            totalSpeakingTime = df.loc[member]['totalSpeakingTime']
            if member_names:
                member = member_names[member]
            if member not in members_avg:
                members_avg[member] = {'turns': totalTurns, 'speak': totalSpeakingTime}
            else:
                members_avg[member]['turns'] += totalTurns
                members_avg[member]['speak'] += totalSpeakingTime
        total_turns_all += df['totalTurns'].sum()
        total_speaking_time_all += df['totalSpeakingTime'].sum()

        # Convert to percentages
        df['totalSpeakingTime'] = (df['totalSpeakingTime'] / df['totalSpeakingTime'].sum()) * 100
        # Excludes time with no one speaking (all members add to 100%)
        df['totalTurns'] = (df['totalTurns'] / df['totalTurns'].sum()) * 100

        for member in df.index.values:
            member_name = member
            if member_names:
                member_name = member_names[member]
            participation_values[member_name] = participation_values.get(member_name, {})
            participation_values[member_name][label] = {'turns': df['totalTurns'][member],
                                                        'speak': df['totalSpeakingTime'][member]}

    # for member in df.index.values:
    for member in participation_values.keys():
        participation_values[member]['Average'] = {'turns': (members_avg[member]['turns'] / total_turns_all) * 100,
                                                   'speak': (members_avg[member]['speak'] / total_speaking_time_all) * 100}
    return participation_values


# plot percentage participation for a member
def participation_plot(df_stitched_all, labels, member=None, member_names=None):
    """
    Creates simple participation chart (no percentages in hover, Tabs with speaking turns, speaking time
    :param df_stitched_all: a list of lists of df_stitched
    :param labels: a list of dates/weeks for which the df_stitched lists are for (excluding 'Average')
    :param member: Member who is viewing the report (serves as base of bar stacks). Either..
    - member key if no member_names dictionary is given, or
    - member name if member_names dictionary is given
    :param member_names: A dictionary mapping member keys to member names (First Last format)
    :return: bokeh plot
    """
    participation_values = percentage_participation(df_stitched_all, labels, member_names=member_names)
    if isinstance(participation_values, basestring):
        participation_values = ast.literal_eval(participation_values)
    if participation_values is None:
        print('ERROR: No participation values found')
        return (None, None)
    # Turn participation_values into format needed for bokeh's stacked Bar Chart
    data_dict = {'labels': [], 'member': [], 'turns': [], 'speak': []}
    # To add an empty bar between Average and rest of bars,
    #  and to add an empty bar at the end for clearer legend positioning.
    labels += ['', 'Average', '.']

    def add_member(member):
        # Add data to dictionary for Bargraph input
        # data_dict['week_num'] += weeks
        data_dict['labels'] += labels
        data_dict['member'] += [member for label in labels]
        for label in labels:
            if label != 'Average':
                if label in participation_values[member]:
                    data_dict['turns'] += [participation_values[member][label]['turns']]
                    data_dict['speak'] += [participation_values[member][label]['speak']]
                else:  # If member had no data for that week
                    data_dict['turns'] += [0]
                    data_dict['speak'] += [0]
            else:
                    data_dict['turns'] += [participation_values[member]['Average']['turns']]
                    data_dict['speak'] += [participation_values[member]['Average']['speak']]
    if member:
        # Add 'You' first so that 'You' is on bottom of bar chart
        add_member(member)
        for key in participation_values:
            if key != member:
                add_member(key)
    else:
        for key in participation_values:
            add_member(key)

    plot = stack_bar(data_dict)
    return plot