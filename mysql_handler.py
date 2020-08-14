import config
import pymysql
from datetime import datetime

conn = None

def init_connection():
    global conn
    if conn is None:
        conn = pymysql.connect(
            host = config.get_db_host(),
            user = config.get_db_user(),
            password = config.get_db_password(),
            port = config.get_db_port(),
            db = config.get_db_name(),
            cursorclass = pymysql.cursors.DictCursor
        )


def close_connection():
    global conn
    if conn is not None:
        conn.close()
    conn = None


def get_connection():
    global conn
    if conn is None:
        init_connection()
    return conn


def select_platform_token_by_ids(ids, debug=False):
    if len(ids) < 1:
        print('not enough ids')
        return

    if debug:
        return [{'id': i, 'token': 'token_' + str(i), 'platform': 'fake'} for i in ids]

    ids_str = ','.join(ids)
    query = 'select id, notification_id as token, os_name as platform from devices_hs_id ' \
            'where notification_id is not null and id in ({})'.format(ids_str)
    print('QUERY INTERACTIONS', query)
    res = None
    with get_connection().cursor() as cursor:
        cursor.execute(query)
        res = cursor.fetchall()
    close_connection()
    return res


def set_infected_devices(devices_infected, timestamp_infection, timestamp_analysis, debug=False):
    if debug:
        return True

    if len(devices_infected) < 1:
        return True

    values = ['(\'' + '\',\''.join([str(did),
                        timestamp_infection.strftime(config.get_timestamp_format_string()),
                        timestamp_analysis.strftime(config.get_timestamp_format_string())]) + '\')'
              for did in devices_infected]
    query = 'INSERT IGNORE INTO infected_devices (device_id, infection_timestamp, last_analysis_timestamp) VALUES {}'\
        .format(','.join(values))
    # print(values)
    # print(query)
    with get_connection().cursor() as cursor:
        cursor.execute(query)
    get_connection().commit()
    close_connection()

def set_healed(devices_infected, timestamp_healed, debug=False):
    if debug:
        return True

    if len(devices_infected) < 1:
        return True

    values = ['(\'' + '\',\''.join([str(did),
                        timestamp_healed.strftime(config.get_timestamp_format_string()),]) + '\')'
              for did in devices_infected]
    query = 'INSERT IGNORE INTO infected_devices (device_id, last_analysis_timestamp, healed_timestamp) VALUES {}'\
        .format(','.join(values))
    # print(values)
    # print(query)
    with get_connection().cursor() as cursor:
        cursor.execute(query)
    get_connection().commit()
    close_connection()


    


if __name__ == "__main__":
    set_infected_devices([1, 2, 3], datetime.now(), datetime.now())
