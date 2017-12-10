import json
import os
import datetime


def is_meeting_metadata(json_record):
    """
    returns true if given record is a header
    :param json_record:
    :return:
    """
    if 'startTime' in json_record:
        return True
    elif "type" in json_record and json_record["type"] == "meeting started":
        return True
    else:
        return False


def meeting_log_version(meeting_metadata):
    """
    returns version number for a given meeting metadata. This is done for
    backward compatibility with meetings that had no version number
    :param meeting_metadata:
    :return:
    """
    log_version = '1.0'
    if 'data' in meeting_metadata and 'log_version' in meeting_metadata['data']:
            log_version = meeting_metadata['data']['log_version']
    return log_version


def peek_line(f):
    """Returns the content of the next line of a fileobject, without advancing the
    cursor."""
    pos = f.tell()
    line = f.readline()
    f.seek(pos)
    return line


def extract_log_version(fileobject):
    """Extracts the metadata from a fileobject, if present."""
    # Check if first line contains metadata
    metadata = json.loads(peek_line(fileobject))
    
    if is_meeting_metadata(metadata):
        # If it does, extract the log version from there
        fileobject.readline()  # Skip first line
        return meeting_log_version(metadata)
    
    else:
        return None


def split_raw_data_by_day(fileobject, target, kind, log_version=None):
    """Splits the data from a raw data file into a single file for each day.

    Parameters
    ----------
    fileobject : object, supporting tell, readline, seek, and iteration.
        The raw data to be split, for instance, a file object open in read mode.

    target : str
        The directory into which the files will be written.  This directory must
        already exist.

    kind : str
        The kind of data being extracted, either 'audio' or 'proximity'.

    log_version : str
        The log version, in case no metadata is present.
    """
    # The days fileobjects
    # It's a mapping from iso dates (e.g. '2017-07-29') to fileobjects
    days = {}
    
    # Extract log version from metadata, if present
    log_version = extract_log_version(fileobject) or log_version

    if log_version not in ('1.0', '2.0'):
        raise Exception('file log version was not set and cannot be identified')

    if log_version in ('1.0'):
        raise Exception('file version '+str(log_version)+'is no longer supported')

    # Read each line
    for line in fileobject:
        data = json.loads(line)

        # Keep only relevant data
        if not data['type'] == kind + ' received':
            continue

        # Extract the day from the timestamp
        day = datetime.date.fromtimestamp(data['data']['timestamp']).isoformat()

        # If no fileobject exists for that day, create one
        if day not in days:
            days[day] = open(os.path.join(target, day), 'a')

        # Write the data to the corresponding day file
        json.dump(data, days[day])
        days[day].write('\n')
    
    # Free the memory
    for f in days.values():
        f.close()

