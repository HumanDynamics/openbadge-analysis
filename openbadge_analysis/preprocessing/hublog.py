from __future__ import absolute_import, division, print_function

import pandas as pd
import re
import ast

log_pattern = re.compile("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ - \w+ - .*$")


def _is_legal_log_line(line):
    return log_pattern.match(line) is not None


def _hublog_read_scan_line(line):
    """ Parses a single scan line from a hub log

    Parameters
    ----------
    line : str
        A single line of log file

    Returns
    -------
    dictionary:
        Scan data of a single badge. When an advertisement packet was available,
        it will include voltage, sync and recording status, etc. If the lien is
        not a scan line, it will return None

    """
    # Removing ANSI from line (colors)
    ansi_escape = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
    line = ansi_escape.sub('', line)

    # remove end of line
    line = line.rstrip("\n\r")

    # Filter out rows with illegal structure
    if not _is_legal_log_line(line):
        return None


    # parse
    data = line.split(" - ")[2]

    if not data.startswith("Found"):
        return None

    scan_data = {}
    adv_payload_raw = data.split("adv_payload': ")[1][0:-1]
    adv_payload = ast.literal_eval(adv_payload_raw)

    if not adv_payload:
        adv_payload = {'proximity_status': None, \
                       'sync_status': None, \
                       'audio_status': None, \
                       'mac': None, \
                       'badge_id': None, \
                       'voltage': None, \
                       'status_flags': None, \
                       'project_id': None}

    scan_data.update(adv_payload)
    scan_data['mac'] = data.split(" ")[1][0:-1]
    scan_data['rssi'] = data.split(": ")[2].split(",")[0]
    scan_data['datetime'] = line.split(" - ")[0]
    scan_data['adv_payload'] = re.sub('[ :\'\[]', '', adv_payload_raw)  # shortenning it
    return scan_data


def hublog_scans(fileobject, log_tz, tz='US/Eastern'):
    """Creates a DataFrame of hub scans.

    Parameters
    ----------
    fileobject : file or iterable list of str
        The raw log file from a hub.

    log_tz : str
        The time zone used in the logfile itself

    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    Returns
    -------
    pd.Series :
        A scan record with mac, rssi, and device status (if available)
    """

    def readfile(fileobject):
        for line in fileobject:
            line_num = line_num + 1
            data = _hublog_read_scan_line(line)
            if data:
                yield (data['datetime'],
                       str(data['mac']),
                       float(data['rssi']),
                       data['voltage'],
                       data['badge_id'],
                       data['project_id'],
                       data['sync_status'],
                       data['audio_status'],
                       data['proximity_status'],
                       )
            else:
                continue  # skip unneeded lines

    df = pd.DataFrame(readfile(fileobject), columns=['datetime', 'mac', 'rssi', 'voltage', 'badge_id', \
                                                     'project_id', 'sync_status', 'audio_status', \
                                                     'proximity_status'])

    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True) \
        .dt.tz_localize(log_tz).dt.tz_convert(tz)

    # Sort
    df = df.set_index('datetime')
    df.sort_index(inplace=True)
    return df


def _hublog_read_reset_line(line):
    """ Parses a single reset line from a hub log

    Parameters
    ----------
    line : str
        A single line of log file

    Returns
    -------
    dictionary:
        Parses a sync event - when badge was previously not synced and was sent a new date

    """
    # remove end of line
    line = line.rstrip("\n\r")

    # Filter out rows with illegal structure
    if not _is_legal_log_line(line):
        return None

    # Parse data
    data = line.split(" - ")[2]

    if not data.endswith("Badge previously unsynced."):
        return None

    sync_data = {}
    sync_data['datetime'] = line.split(" - ")[0]
    sync_data['mac'] = data[1:18]
    return sync_data


def hublog_resets(fileobject, log_tz, tz='US/Eastern'):
    """Creates a DataFrame of reset events - when badge were previously not synced and
        the hub sent a new date

    Parameters
    ----------
    fileobject : file or iterable list of str
        The raw log file from a hub.

    log_tz : str
        The time zone used in the logfile itself

    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    Returns
    -------
    pd.Series :
        A record with mac and timestamp
    """
    def readfile(fileobject):
        for line in fileobject:
            data = _hublog_read_reset_line(line)
            if data:
                yield (data['datetime'],
                       str(data['mac']),
                       )
            else:
                continue  # skip unneeded lines

    df = pd.DataFrame(readfile(fileobject), columns=['datetime', 'mac'])

    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True) \
        .dt.tz_localize(log_tz).dt.tz_convert(tz)

    # Sort
    df = df.set_index('datetime')
    df.sort_index(inplace=True)
    return df
