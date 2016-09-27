import pandas as pd
import datetime
import json
import hashlib
import random
import ast
import os
import sys
import argparse
import math

from bokeh.plotting import *
from bokeh.models import ColumnDataSource, HoverTool, Legend, DatePicker, CustomJS
from bokeh.layouts import widgetbox, row, column
from bokeh.models.tickers import FixedTicker
from bokeh.palettes import brewer

# add the 'src' directory as one where we can import modules
src_dir = os.path.join(os.getcwd(), os.pardir, '../src')
sys.path.append(src_dir)
import openbadge_analysis as ob


def stack_bar(participation_values, member, labels, metric, choose):
    """
    Creates a stacked bar graph showing percentages of participation for each member for each day/week + hover
    :param participation_values: Dict{ Member : { date : {'turns': float, 'speak, float } }, Member : ... }
    :param member: Member who is viewing the report (serves as base of bar stacks). Either..
    - member key if no member_names dictionary is given, or
    - member name if member_names dictionary is given
    :param labels: List[date1, date2, ..., 'Average']
    :param metric: turns' or 'speak' (Whether you want to use Turns or Speaking Time)
    :param choose: Whether or not to add Date Picker to choose dates
    :return: bokeh plot
    """
    colors = brewer['Set2'][len(participation_values)]
    members = {}  # To order members
    data = {}
    bottoms = {}
    color_members = {}
    for date in labels:
        bottoms[date] = 0
    if member:
        # Order the members so that 'member' is always on the bottom of the stacked bar graph, and so that bars
        # will always be stacked in the same order.
        # members is a dictionary, e.g. {'0': 'Member1', '1': 'Member2', etc... }
        # color_members is a dictionary, e.g. {'Member1': 'color_hex_value_1', 'Member2': 'color_hex_value_2', etc... }
        i = 1
        for member_name in participation_values:
            if member_name == member:
                members[str(0)] = member_name
                color_members[member_name] = colors[0]
            else:
                members[str(i)] = member_name
                color_members[member_name] = colors[i]
                i += 1
    else:
        i = 0
        for member_name in participation_values:
            members[str(i)] = member_name
            color_members[member_name] = colors[i]
            i += 1

    total_particip = {'all': 0}
    for member in participation_values:
        total_particip[member] = 0
        for date in labels:
            if date in participation_values[member]:
                particip = participation_values[member][date][metric]
            else:
                particip = 0
            total_particip[member] += particip
            total_particip['all'] += particip
    for member in participation_values:
        participation_values[member]['Average'] = {}
        participation_values[member]['Average'][metric] = total_particip[member] / total_particip['all'] * 100

    x = 1
    for date in labels:
        data[date] = {}
        data[date]['date'] = []
        data[date]['x'] = []
        data[date]['y'] = []
        data[date][metric] = []
        data[date]['member'] = []
        data[date]['color'] = []
        for i in range(len(members)):
            member = members[str(i)]
            if date in participation_values[member]:
                particip = participation_values[member][date][metric]
            else:
                particip = 0
            data[date]['color'].append(color_members[member])
            data[date]['date'].append(date)
            data[date]['x'].append(x)
            data[date][metric].append(particip)
            data[date]['y'].append(bottoms[date] + particip/2)
            data[date]['member'].append(member)
            bottoms[date] += particip
        x += 1

    src_all = {}
    for date in data:
        src_all[date] = ColumnDataSource(data=data[date])

    source_p_values = ColumnDataSource(data=participation_values)
    source_colors = ColumnDataSource(data=color_members)
    source_members = ColumnDataSource(data=members)
    source_labels = ColumnDataSource(data=dict(labels=labels[:-2]))

    height = 500
    width = 800

    if metric == 'turns':
        hover = HoverTool(
            tooltips="""
                <div>
                <div>
                <span style="font-size: 17px; font-weight: bold;">@member</span>
                </div>
                <div>
                <span style="font-size: 17px;">Date: </span>
                <span style="font-size: 17px; color: purple;">@date</span>
                </div>
                <div>
                <span style="font-size: 17px; font-weight: bold; color: green;">@turns%</span>
                <span style="font-size: 17px;"> Speaking Turns</span>
                </div>
                </div>
            """
        )

        p = figure(title='Your Percentage of Participation (Based on Number of Speaking Turns)',
                   plot_width=width, plot_height=height, x_range=[0.5,len(labels)+0.5], y_range=[-7,101],
                   tools=[hover], toolbar_location='above', sizing_mode='scale_width')
        p.yaxis.axis_label = 'Your Percentage of Participation (Speaking Turns)'

    elif metric == 'speak':
        hover = HoverTool(
            tooltips="""
                <div>
                <div>
                <span style="font-size: 17px; font-weight: bold;">@member</span>
                </div>
                <div>
                <span style="font-size: 17px;">Date: </span>
                <span style="font-size: 17px; color: purple;">@date</span>
                </div>
                <div>
                <span style="font-size: 17px; font-weight: bold; color: green;">@speak%</span>
                <span style="font-size: 17px;"> Speaking Time</span>
                </div>
                </div>
            """
        )

        p = figure(title='Your Percentage of Participation (Based on Amount of Speaking Time)',
                   plot_width=width, plot_height=height, x_range=[0.5,len(labels)+0.5], y_range=[-15,101],
                   tools=[hover], toolbar_location='above', sizing_mode='scale_width')
        p.yaxis.axis_label = 'Your Percentage of Participation (Speaking Time)'


    legends = []
    rects = []
    texts = []
    dates = data.keys()
    dates.sort()
    rect_avg = None
    for date in dates:
        rec = p.rect(source=src_all[date], width=.8, height=metric, x='x', y='y',
                     fill_color='color', line_color='white')
        txt = p.text(
            source=ColumnDataSource(data={'date': [data[date]['date'][0]], 'x': [data[date]['x'][0]]}),
            text='date', x='x', y=-8, text_align='center', angle=.785)  # radians
        if date == 'Average':
            rect_avg = rec
        else:
            if date != '':
                rects.append(rec)
                texts.append(txt)

    # For legend
    for member in color_members:
        sq = p.square(110, 110, size=0, color=color_members[member])
        legends.append((member, [sq]))

    p.grid.grid_line_alpha = 0.4

    label_font_style = 'normal'  # 'italic', 'bold'
    p.xaxis.axis_label = 'Date'
    p.xaxis.axis_label_text_font_size = str(height/50) + 'pt'
    p.xaxis.major_label_text_font_size = '0pt'
    p.xaxis.axis_label_text_font_style = label_font_style
    p.xaxis.ticker=FixedTicker(ticks=[0])

    p.yaxis.major_label_text_font_size = str(height/50) + 'pt'
    p.yaxis.axis_label_text_font_size = str(height/50) + 'pt'
    p.yaxis.axis_label_text_font_style = label_font_style


    legend = Legend(legends=legends, location=(0, -30))

    p.add_layout(legend, 'right')

    if choose:
        date_pickers = []
        for i in range(len(labels) - 2):
            source_i = ColumnDataSource(data={'i':[i]})
            if metric == 'turns':
                cb = CustomJS(args={'source_p_values': source_p_values,
                                    'source_colors': source_colors, 'source_labels': source_labels,
                                    'source_members': source_members, 'txt_source': texts[i].data_source,
                                    'source_i': source_i, 'r_source_avg': rect_avg.data_source,
                                    'r_source': rects[i].data_source}, code="""
                    var d = cb_obj.get('value');
                    var dMs = Date.parse(d);
                    var dt = new Date(dMs);
                    var day = dt.getDate();
                    day_str = day.toString();
                    if (day < 10){
                      day_str = '0' + day.toString();
                    };
                    var month = dt.getMonth() + 1;  // Month is 1 less than actual picked month for some reason
                    console.log(month);
                    month_str = month.toString();
                    if (month < 10) {
                      month_str = '0' + month.toString();
                    };
                    var date_str = month_str + '/' + day_str;

                    var labels_data = source_labels.get('data');
                    var i = source_i.get('data')['i'][0];
                    labels_data['labels'].splice(i, 1, date_str);
                    var labels = labels_data['labels'];
                    console.log(labels);
                    var p_data = source_p_values.get('data');


                    var total_turns = {'all': 0};
                    for (member in p_data) {
                        total_turns[member] = 0;
                        for (index in labels) {
                            var turns = 0;
                            var date = labels[index];
                            console.log(p_data[member]);
                            if (date in p_data[member]) {
                                turns = p_data[member][date]['turns'];
                            }
                            console.log(turns);
                            total_turns[member] += turns;
                            total_turns['all'] += turns;
                            console.log(total_turns[member]);
                            console.log(total_turns['all']);
                        }
                    }
                    for (member in p_data) {
                        p_data[member]['Average'] = {};
                        console.log(total_turns[member]);
                        p_data[member]['Average']['turns'] = total_turns[member] / total_turns['all'] * 100;
                    }

                    var colors = source_colors.get('data');
                    var members = source_members.get('data');
                    new_data = {}
                    bottom = 0
                    new_data['date'] = []
                    new_data['y'] = []
                    new_data['turns'] = []
                    new_data['member'] = []
                    new_data['color'] = []
                    for (i=0; i<Object.keys(members).length; i++){
                        member = members[i.toString()];
                        var turns = 0;
                        if (date_str in p_data[member]) {
                            turns = p_data[member][date_str]['turns'];
                        };
                        new_data['color'].push(colors[member]);
                        new_data['date'].push(date_str);
                        new_data['turns'].push(turns);
                        new_data['y'].push(bottom + turns/2);
                        new_data['member'].push(member);
                        bottom += turns;
                    }

                    new_avg_data = {}
                    bottom = 0
                    new_avg_data['date'] = []
                    new_avg_data['y'] = []
                    new_avg_data['turns'] = []
                    new_avg_data['member'] = []
                    new_avg_data['color'] = []
                    for (i=0; i<Object.keys(members).length; i++){
                        member = members[i.toString()];
                        turns = p_data[member]['Average']['turns'];
                        new_avg_data['color'].push(colors[member]);
                        new_avg_data['date'].push('Average');
                        new_avg_data['turns'].push(turns);
                        new_avg_data['y'].push(bottom + turns/2);
                        new_avg_data['member'].push(member);
                        bottom += turns;
                    }

                    var r_data = r_source.get('data');
                    var r_avg_data = r_source_avg.get('data');
                    var txt_data = txt_source.get('data');
                    for (key in new_data) {
                        r_data[key] = new_data[key];
                        txt_data[key] = new_data[key];
                        r_avg_data[key] = new_avg_data[key];
                    }
                    console.log(r_avg_data);
                    r_source.trigger('change');
                    r_source_avg.trigger('change');
                    txt_source.trigger('change');
                    """
                             )
            elif metric == 'speak':
                cb = CustomJS(args={'source_p_values': source_p_values,
                                    'source_colors': source_colors, 'source_labels': source_labels,
                                    'source_members': source_members, 'txt_source': texts[i].data_source,
                                    'source_i': source_i, 'r_source_avg': rect_avg.data_source,
                                    'r_source': rects[i].data_source}, code="""
                    var d = cb_obj.get('value');
                    var dMs = Date.parse(d);
                    var dt = new Date(dMs);
                    var day = dt.getDate();
                    day_str = day.toString();
                    if (day < 10){
                      day_str = '0' + day.toString();
                    };
                    var month = dt.getMonth() + 1;  // Month is 1 less than actual picked month for some reason
                    console.log(month);
                    month_str = month.toString();
                    if (month < 10) {
                      month_str = '0' + month.toString();
                    };
                    var date_str = month_str + '/' + day_str;

                    var labels_data = source_labels.get('data');
                    var i = source_i.get('data')['i'][0];
                    labels_data['labels'].splice(i, 1, date_str);
                    var labels = labels_data['labels'];
                    console.log(labels);
                    var p_data = source_p_values.get('data');


                    var total_turns = {'all': 0};
                    for (member in p_data) {
                        total_turns[member] = 0;
                        for (index in labels) {
                            var turns = 0;
                            var date = labels[index];
                            console.log(p_data[member]);
                            if (date in p_data[member]) {
                                turns = p_data[member][date]['speak'];
                            }
                            console.log(turns);
                            total_turns[member] += turns;
                            total_turns['all'] += turns;
                            console.log(total_turns[member]);
                            console.log(total_turns['all']);
                        }
                    }
                    for (member in p_data) {
                        p_data[member]['Average'] = {};
                        console.log(total_turns[member]);
                        p_data[member]['Average']['speak'] = total_turns[member] / total_turns['all'] * 100;
                    }

                    var colors = source_colors.get('data');
                    var members = source_members.get('data');
                    new_data = {}
                    bottom = 0
                    new_data['date'] = []
                    new_data['y'] = []
                    new_data['speak'] = []
                    new_data['member'] = []
                    new_data['color'] = []
                    for (i=0; i<Object.keys(members).length; i++){
                        member = members[i.toString()];
                        var turns = 0;
                        if (date_str in p_data[member]) {
                            turns = p_data[member][date_str]['speak'];
                        };
                        new_data['color'].push(colors[member]);
                        new_data['date'].push(date_str);
                        new_data['speak'].push(turns);
                        new_data['y'].push(bottom + turns/2);
                        new_data['member'].push(member);
                        bottom += turns;
                    }

                    new_avg_data = {}
                    bottom = 0
                    new_avg_data['date'] = []
                    new_avg_data['y'] = []
                    new_avg_data['speak'] = []
                    new_avg_data['member'] = []
                    new_avg_data['color'] = []
                    for (i=0; i<Object.keys(members).length; i++){
                        member = members[i.toString()];
                        turns = p_data[member]['Average']['speak'];
                        new_avg_data['color'].push(colors[member]);
                        new_avg_data['date'].push('Average');
                        new_avg_data['speak'].push(turns);
                        new_avg_data['y'].push(bottom + turns/2);
                        new_avg_data['member'].push(member);
                        bottom += turns;
                    }

                    var r_data = r_source.get('data');
                    var r_avg_data = r_source_avg.get('data');
                    var txt_data = txt_source.get('data');
                    for (key in new_data) {
                        r_data[key] = new_data[key];
                        txt_data[key] = new_data[key];
                        r_avg_data[key] = new_avg_data[key];
                    }
                    console.log(r_avg_data);
                    r_source.trigger('change');
                    r_source_avg.trigger('change');
                    txt_source.trigger('change');
                    """
                              )

            m = int(labels[i].split('/')[0])
            d = int(labels[i].split('/')[1])
            date_pickers.append(DatePicker(title='Day ' + str(i+1), min_date=datetime.datetime(2016,6,1),
                                max_date=datetime.datetime.now(),
                                value=datetime.datetime(datetime.datetime.now().year,m,d),
                                callback=cb,
                                width=width/5, height=200)
                                )
        return column(children=[p, row(children=date_pickers)], sizing_mode='scale_width')

    else:
        return p


def percentage_participation(df_stitched_all, labels, member_names=None):
    """
    Process data for percentage participation for a group
    :param df_stitched_all: a list of lists of df_stitched
    :param labels: a list of dates/weeks for which the df_stitched lists are for
    :param member_names: A dictionary mapping member keys to member names (First Last format)
    :return: participation_values Dict{ Member : { date : {'turns': float, 'speak, float } }, Member : ... }
    turns include self-turns
    """
    participation_values = {}

    for i in range(len(df_stitched_all)):
        label = labels[i]
        df_stitched_list = df_stitched_all[i]
        if len(df_stitched_list) == 0:
            print('No meetings for ' + str(label))
            continue
        df = pd.DataFrame()
        print('Generating percentage participation data for  ' + str(label))
        for df_stitched in df_stitched_list:
            df_turns = ob.total_turns(df_stitched)
            df_turns.set_index('member', inplace=True)
            df = df.add(df_turns, fill_value=0)

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

    return participation_values


def participation_chart(df_stitched_all, labels, metric, choose=False, member=None, member_names=None):
    """
    Creates participation chart (with hover)
    :param df_stitched_all: a list of lists of df_stitched
    :param labels: a list of dates/weeks for which the df_stitched lists are for (excluding 'Average')
    :param metric: 'turns' or 'speak' (Whether you want to use Turns or Speaking Time)
    :param choose: Whether or not to add Date Picker to choose dates
    :param member: Member who is viewing the report (serves as base of bar stacks)
    :param member_names: A dictionary mapping member keys to member names (First Last format)
    :return: bokeh plot
    """
    participation_values = percentage_participation(df_stitched_all, labels, member_names)
    if isinstance(participation_values, basestring):
        participation_values = ast.literal_eval(participation_values)
    if participation_values is None:
        print('ERROR: No participation values found for Member ' + member)
        return None, None
    labels += ['', 'Average']
    plot = stack_bar(participation_values, member, labels, metric, choose)
    return plot
