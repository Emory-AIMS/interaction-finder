
import sys
import math
import json
import boto3
import config
import multiprocessing
from enum import Enum
from datetime import datetime, timedelta
from dateutil import parser

# https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html
# https://www.msp360.com/resources/blog/how-to-use-s3-select-feature-amazon/
# https://iotandelectronics.wordpress.com/2016/10/07/how-to-calculate-distance-from-the-rssi-value-of-the-ble-beacon/

# https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html

BUCKET = config.get_s3_bucket_name()
NUMBER_DEVICES = 100000
PAIRS_SHINGLES_DEVICE = 5
METER_DISTANCE_RSSI = -69
N_POWER_DISTANCE = 2
distance_levels = ['i', 'n', 'f']
MINIMUM_INTERVAL_TIME = 30


class CsvFormat(Enum):
    my_id = 1
    other_id = 2
    timestamp_start = 3
    timestamp_end = 4
    interval = 5
    rssi = 6
    distance = 7
    latitude = 8
    longitude = 9
    platform = 10


def padding_zeroes(number, length_string):
    """
    Return a string of given length from a number, adding all zeroes needed
    :param number:
    :param length_string:
    :return:
    """
    return str(number).zfill(length_string)


def get_distance(rssi):
    """
    Return the distance int mt given :rssi:
    :param rssi:
    :return:
    """
    return math.pow(10, (METER_DISTANCE_RSSI - rssi) / (math.pow(10, N_POWER_DISTANCE)))


def get_rssi_from_distance(distance):
    """
    Estimate the RSSI given the :distance: expressed in mt
    :param distance:
    :return:
    """
    return - (math.log(distance, 10) * math.pow(10, N_POWER_DISTANCE) - METER_DISTANCE_RSSI)


def build_path_device(device_id):
    """
    Helper which build the S3 hierarchical path for a given device id
    :param device_id:
    :return:
    """
    padding_device = PAIRS_SHINGLES_DEVICE * 2
    s = padding_zeroes(int(int(device_id) / NUMBER_DEVICES), padding_device)
    res = ''
    for i in range(0, padding_device, 2):
        res += s[i: i+2] + '/'
    return res


def get_paths_s3(device_id, days_back=config.days_look_back()):
    """
    Return the list of folders for the given :device_id: going back to :days_back: days
    :param device_id: device id
    :param days_back: days to go back
    :return:
    """
    today = datetime.now()
    paths = []
    for i in range(days_back + 1):
        d = today - timedelta(days=i)
        paths.append(
            str(d.year) + '/' +
            padding_zeroes(d.month, 2) + '/' +
            padding_zeroes(d.day, 2) + '/' +
            build_path_device(device_id)
        )
    return paths


def list_files(devices, last_timestamp_filter=None, days_back=config.days_look_back()):
    """
    Return the list of files for the given :device_id: going back :days_back: days
    :param devices:
    :param last_timestamp_filter:
    :param days_back:
    :return:
    """
    client = boto3.client('s3')
    if last_timestamp_filter is not None:
        print('gonna filtering out files before', last_timestamp_filter)
    file_names = set()
    for device_id in devices:
        paths = get_paths_s3(device_id, days_back=days_back)
        for p in paths:
            continuation = None
            iterate = True
            while iterate:
                if continuation is not None:
                    response = client.list_objects_v2(
                        Bucket=BUCKET,
                        MaxKeys=1000,
                        Prefix=p,
                        ContinuationToken=continuation
                    )
                else:
                    response = client.list_objects_v2(
                        Bucket=BUCKET,
                        MaxKeys=1000,
                        Prefix=p
                    )
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    iterate = response['IsTruncated']
                    if 'Contents' in response:
                        for e in response['Contents']:
                            file_name_to_add = e['Key']
                            if last_timestamp_filter is not None:
                                if '/' in file_name_to_add and '_' in file_name_to_add:
                                    try:
                                        dir_tree = file_name_to_add.split('/')
                                        csv_file_name = dir_tree[len(dir_tree)-1]
                                        date_file = parser.parse(csv_file_name.split('_')[0])
                                        if date_file < last_timestamp_filter:
                                            # file to skip since already analyzed
                                            print('skipping file', file_name_to_add)
                                            continue
                                    except:
                                        print('EXCEPTION on parsing datetime in filename ' + file_name_to_add)
                            file_names.add(file_name_to_add)
                    # iterate = False
                    if 'NextContinuationToken' in response:
                        print('CONTINATION TOKEN')
                        continuation = response['NextContinuationToken']
                else:
                    print("Error", response)
                    break
    return list(file_names)


def build_query(devices, filter_inter, interactions_further=None):
    """
    Build the query used to run on S3
    :param filter_inter:
    :return:
    """

    id_2_found = CsvFormat.other_id.value
    if interactions_further is not None:
        id_2_found = CsvFormat.my_id.value

    query = 'SELECT i._{} as id, CAST(i._{} as int) as timestamp_start, CAST(i._{} as float) as interval_time ' \
            'FROM S3Object i WHERE CAST(i._{} as float) >= {}'.format(
                id_2_found,
                CsvFormat.timestamp_start.value,
                CsvFormat.interval.value,
                CsvFormat.interval.value,
                MINIMUM_INTERVAL_TIME)

    if 'distance' in filter_inter:
        if 'max' in filter_inter['distance']:
            query += ' and CAST(i._{} as float) <= {}'.format(CsvFormat.distance_type.value,
                                                            filter_inter['distance']['max'])
        if 'min' in filter_inter['distance']:
            query += ' and CAST(i._{} as float) >= {}'.format(CsvFormat.distance_type.value,
                                                            filter_inter['distance']['min'])

    if interactions_further is not None:
        condos = []
        for inter in interactions_further:
            orcond = '(i._{}=\'{}\' and (CAST(i._{} as float) between {} and {} or CAST(i._{} as float) between {} and {}))'\
                .format(CsvFormat.other_id.value,
                        inter['id'],
                        CsvFormat.timestamp_start.value,
                        inter['timestamp_start'],
                        int(inter['timestamp_start'] + inter['interval_time']),
                        CsvFormat.timestamp_end.value,
                        inter['timestamp_start'],
                        int(inter['timestamp_start'] + inter['interval_time'])
                        )
            condos.append(orcond)
        if len(condos) > 0:
            query += ' and ' + ' or '.join(condos)
    else:
        query += ' and i._{} in (\'{}\')'.format(
            CsvFormat.my_id.value,
            '\',\''.join([str(x) for x in devices])
        )

    return query


def query_file(file_name, query):
    client = boto3.client('s3')
    interaction_id2_interactions = {}
    all_interactions = set()

    response = client.select_object_content(
        Bucket=BUCKET,
        Key=file_name,
        Expression=query,
        ExpressionType='SQL',
        InputSerialization={
            'CSV': {
                'FileHeaderInfo': 'NONE'
            }
        },
        OutputSerialization={
            'JSON': {
                # 'RecordDelimiter': '\n'
            }
        }
    )
    payload = ''
    for event in response['Payload']:
        if 'Records' in event:
            payload += event['Records']['Payload'].decode('utf-8')

    payloads = payload.split('\n')
    for p in payloads:
        if p.strip() == '':
            continue
        j = json.loads(p)
        if j['id'] not in interaction_id2_interactions:
            interaction_id2_interactions[j['id']] = []
        interaction_id2_interactions[j['id']].append(j)

        all_interactions.add(j['id'])
    return interaction_id2_interactions, all_interactions


def select_s3_interactions(devices, filters, interactions_further=None, last_timestamp_filter=None, days_back=config.days_look_back()):
    """
    Return a list of interactions for the specified :device_id: for every :filters:
    :param devices:
    :param filters:
    :param last_timestamp_filter:
    :param days_back:
    :return: dictionary where the key is the filter_id and the value is the list of interactions, unique_interactions
    """

    file_names = list_files(devices, last_timestamp_filter=last_timestamp_filter, days_back=days_back)
    """
    filter_id2connections = {
        filter_id: {
            device_id: [interactions]
        }
    }
    """
    filter_id2connections = {}
    all_interactions = set()

    for fil in filters:
        interactions = None
        if interactions_further is not None and fil['filter_id'] in interactions_further:
            interactions = interactions_further[fil['filter_id']]
        query = build_query(devices, fil, interactions)
        print('Iterating for filter', fil)
        print(query)
        # interactions = set()
        interaction_id2_interactions = {}
        print('gonna query {} files'.format(len(file_names)))

        counter = 0
        pool_size = config.get_number_processes()
        processes = []

        ## Define an empty pool whith maximal concurrent processes
        pool = multiprocessing.Pool(processes=pool_size)

        # PREPOPULATE PROCESSES
        for i in range(0, min(pool_size, len(file_names))):
            processes.append(pool.apply_async(query_file, args=(file_names[counter], query,)))
            counter += 1

        while True:
            for ip in range(len(processes) - 1, -1, -1):
                process = processes[ip]
                if process.ready():
                    # todo: sometimes the next process.get arise an error
                    interaction_id2_interactions_partial, unique_interactions = process.get()
                    all_interactions = all_interactions.union(unique_interactions)
                    for iid, its in interaction_id2_interactions_partial.items():
                        if iid not in interaction_id2_interactions:
                            interaction_id2_interactions[iid] = []
                        for item in its:
                            interaction_id2_interactions[iid].append(item)
                    processes.pop(ip)
                    if counter >= len(file_names):
                        continue
                    processes.append(pool.apply_async(query_file, args=(file_names[counter], query,)))
                    counter += 1
            if len(processes) == 0:
                break
        filter_id2connections[fil['filter_id']] = interaction_id2_interactions

    return filter_id2connections, list(all_interactions)


def aggregate_result_select(filter_id_2_device_id_2_interactions, filter_id_2_filter, round_interactions):
    filter_id_2_connections = {}
    for fid, device_id_2_interactions in filter_id_2_device_id_2_interactions.items():
        # print('FID', fid)
        for did, interactions in device_id_2_interactions.items():
            # print('DID', did)
            aggregated = post_aggregate_interactions(interactions, filter_id_2_filter[fid], round_interactions)
            if aggregated is not None:
                if fid not in filter_id_2_connections:
                    filter_id_2_connections[fid] = []
                filter_id_2_connections[fid].append(aggregated)
    return filter_id_2_connections


# def read_s3(devices, filters, round_interactions=None, last_timestamp_filter=None, firstRun=True, days_back=config.days_look_back(), max_retries=5):
#     try:
#         return read_s3_private(devices, filters, round_interactions=round_interactions,
#                            last_timestamp_filter=last_timestamp_filter, firstRun=firstRun, days_back=days_back)
#     except Exception as e:
#         print("RAISING EXCEPTION", max_retries)
#         if max_retries > 0:
#             return read_s3(devices, filters, round_interactions=round_interactions, last_timestamp_filter=last_timestamp_filter,
#                            firstRun=firstRun, days_back=days_back, max_retries=max_retries-1)
#         else:
#             raise e


def read_s3(devices, filters, round_interactions=None, last_timestamp_filter=None, firstRun=True, days_back=config.days_look_back()):
    filter_id_2_filter = {}
    for fil in filters:
        filter_id_2_filter[fil['filter_id']] = fil
    filter_id_2_device_id_2_interactions, unique_interactions = select_s3_interactions(devices,
                                                      filters,
                                                      interactions_further=None,
                                                      last_timestamp_filter=last_timestamp_filter,
                                                      days_back=days_back)

    filter_id_2_connections = aggregate_result_select(filter_id_2_device_id_2_interactions,
                                                      filter_id_2_filter,
                                                      round_interactions)
    # second query for iphones:
    filter_id_2_device_id_2_interactions_further, unique_interactions_further = select_s3_interactions(
                                                       list(unique_interactions),
                                                       filters,
                                                       interactions_further=filter_id_2_connections,
                                                       last_timestamp_filter=last_timestamp_filter,
                                                       days_back=days_back)
    filter_id_2_connections_further = aggregate_result_select(filter_id_2_device_id_2_interactions_further,
                                                      filter_id_2_filter,
                                                      round_interactions)
    for filter_id, interactions in filter_id_2_connections_further.items():
        for inter in interactions:
            if int(inter['id']) not in devices:
                # add to filter_id_2_connections
                if filter_id not in filter_id_2_connections:
                    filter_id_2_connections[filter_id] = []
                filter_id_2_connections[filter_id].append(inter)

    for uif in unique_interactions_further:
        if int(uif) not in devices:
            unique_interactions.append(uif)

    res = filter_id_2_connections, unique_interactions
    return res


def post_aggregate_interactions(interactions, fil, round_interactions=None):
    print('aggregate round interactions', interactions, round_interactions)
    if len(interactions) < 1:
        return None
    interaction = None

    for i in interactions:

        if round_interactions is not None and 'ids_timestamp' in round_interactions and i['id'] in round_interactions['ids_timestamp']:
            if i['id'] in list(round_interactions['ids_timestamp'].keys()):
                break

            if i['timestamp_start'] < round_interactions['ids_timestamp'][i['id']]:
                continue

        if interaction is None:
            interaction = {
                'id': i['id'],
                'timestamp_start': i['timestamp_start'],
                'interval_time': i['interval_time']
            }
            continue
        interaction['timestamp_start'] = min(interaction['timestamp_start'], i['timestamp_start'])
        interaction['interval_time'] += i['interval_time']

    if interaction is None:
        return None

    if 'time' in fil:
        if 'min' in fil['time'] and interaction['interval_time'] < fil['time']['min']:
            return None
        if 'max' in fil['time'] and interaction['interval_time'] > fil['time']['max']:
            return None
    print('FINAL INTERACTION', interaction)
    return interaction


def test():
    print("## TEST ##")

    r = read_s3([1806], [
        {
            'filter_id': 1,
            # 'time': {
            #     'min': 1
            # }
        }
    ])
    print("## RESULT ##")
    print(r)


def run(devices, filters, round_interactions, last_timestamp_filter=None, days_back=config.days_look_back()):
    print('Gonna search for', devices)

    r, i = read_s3(devices, filters, round_interactions, last_timestamp_filter=last_timestamp_filter, days_back=config.days_look_back())

    print('read from s3, gonna send interactions', r, i)
    return r, i


if __name__ == '__main__':
    s = datetime.now()
    try:
        test()
    except Exception as ee:
        print(type(ee))
        raise ee
    e = datetime.now()
    print(e-s)