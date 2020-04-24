
import json
import config
import mysql_handler
import s3_select_interactions
from datetime import datetime

BULK_SIZE = 1000

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue
LINK_FILTERS = 'https://covid-filters.s3.amazonaws.com/filters_it.json'


def get_warning_level(status):
    if status == 0:
        return 0
    if status == 1:
        return 4
    if status == 2:
        return 2
    if status == 3:
        return 1
    return 3


def read_filters():
    return {
            1: {
                'filter_id': 1,
                'status': 5,
                # 'time': {
                #     'min': 600
                # },
                # 'distance': {
                #     'min': 5
                # },
                # 'rounds': 3,
                'language': 'it',
                'content': {
                    'it': {
                        'title': 'Nuovo aggiornamento di stato',
                        'shortDescription': 'Apri l\'app per avere maggiori informazioni.',
                        'description': 'Hai avuto contatti con una persona infetta, ti preghiamo di rimanere a casa e proteggere le persone intorno a te.'
                    },
                    'pt-br': {
                        'title': 'Seu status foi atualizado.',
                        'shortDescription': 'Abra o aplicativo para mais informações.',
                        'description': 'Você teve contato com uma pessoa infectada, por favor, fique em casa e proteja as pessoas ao seu redor.'
                    }
                }
            }
        }


def send_messages_and_rounds(messages, filter_id_2_rounds, queue_write, queue_infected, debug=False):
    for m in messages:
        print('sending message', m)
        if not debug:
            queue_write.send_message(MessageBody=json.dumps({'notifications': [m]}))

    print('FILTER 2 ROUNDS', filter_id_2_rounds)
    for fid, r in filter_id_2_rounds.items():
        print('sending round', r)
        if r['previous_round'] < r['max_round']:
            if not debug:
                queue_infected.send_message(MessageBody=json.dumps({'round': r}))


def compute_messages(devices_infected, devices_healed, backup_messages_body, rounds,
                     queue_infected, queue_notification, queue_dead_letters, last_timestamp_filter=None, debug=False):
    time_operation = datetime.now()
    try:
        filters = read_filters()
        if len(devices_infected) > 0:
            messages, filter_id_2_rounds = elaborate(devices_infected, filters, None, current_round=1,
                                                     last_timestamp_filter=last_timestamp_filter, debug=debug)
            send_messages_and_rounds(messages, filter_id_2_rounds, queue_notification, queue_infected, debug=debug)

        for r in rounds:
            print('ELABORATING ROUND', r)
            messages, filter_id_2_rounds = elaborate(list(r['ids_timestamp'].keys()), filters, r,
                                                     current_round=r['previous_round']+1,
                                                     last_timestamp_filter=last_timestamp_filter, debug=debug)
            send_messages_and_rounds(messages, filter_id_2_rounds, queue_notification, queue_infected, debug=debug)

        print("GONNA ELABORATE MESSAGES infected", devices_infected)
        # send messages to input devices
        messages_infected = elaborate_input_devices(devices_infected, config.get_status_infected(), debug=debug)
        for i in range(0, len(messages_infected), 50):
            print(messages_infected[0])
            if not debug:
                queue_notification.send_message(MessageBody=json.dumps({'notifications': messages_infected[i:i + 50]}))

        print('GONNA save infected devices')
        mysql_handler.set_infected_devices(devices_infected, time_operation, time_operation, debug=debug)

        print("GONNA ELABORATE MESSAGES healed", devices_healed)
        messages_healed = elaborate_input_devices(devices_healed, config.get_status_healed(), debug=debug)
        for i in range(0, len(messages_healed), 50):
            print(messages_healed[0])
            if not debug:
                queue_notification.send_message(MessageBody=json.dumps({'notifications': messages_healed[i:i + 50]}))
    except Exception as e:
        print('EXCEPTION', e)
        for b in backup_messages_body:
            if not debug:
                queue_dead_letters.send_message(MessageBody=json.dumps(b))
        # raise e


def elaborate_input_devices(devices, st, debug=None):
    interaction2info = {}
    res = mysql_handler.select_platform_token_by_ids(devices, debug=debug)
    if res is None:
        return []
    for r in res:
        interaction2info[str(r['id'])] = r

    messages = []
    for device_id in devices:
        m = {
            'token': interaction2info[str(device_id)]['token'],
            'platform': interaction2info[str(device_id)]['platform'].lower(),
            'data': {
                'status': st,
                'warning_level': get_warning_level(st)
            }
        }
        messages.append(m)

    return messages


def elaborate(devices, filters, round_interactions, current_round=1, last_timestamp_filter=None, debug=False):
    filter_id2interactions, interactions = s3_select_interactions.run(devices,
                                                                      list(filters.values()),
                                                                      round_interactions,
                                                                      last_timestamp_filter=last_timestamp_filter)
    if debug:
        print('FILTER 2 inters', filter_id2interactions)
        print('PLAIN inters', interactions)

    interaction2info = {}
    for i in range(0, len(interactions), BULK_SIZE):
        res = mysql_handler.select_platform_token_by_ids(interactions[i: i + BULK_SIZE], debug=debug)
        # print(res)
        if res is None:
            continue
        for r in res:
            interaction2info[str(r['id'])] = r

    messages = []
    if debug:
        print('INTERACTIONS info mysql', interaction2info)
    filter_id_2_rounds = {}
    for fid, inters in filter_id2interactions.items():
        for interaction in inters:
            id_inter = interaction['id']
            if id_inter not in interaction2info:
                continue
            m = {
                'token': interaction2info[id_inter]['token'],
                'platform': interaction2info[id_inter]['platform'].lower(),
                'data': {
                    'status': filters[fid]['status'],
                    'warning_level': get_warning_level(filters[fid]['status']),
                    'title': filters[fid]['content'][filters[fid]['language']]['title'],
                    'message': filters[fid]['content'][filters[fid]['language']]['shortDescription'],
                    'link': LINK_FILTERS,
                    'language': filters[fid]['language']
                }
            }
            messages.append(m)

            if 'rounds' in filters[fid] and filters[fid]['rounds'] > 1 and current_round < filters[fid]['rounds']:
                # gonna iterate again
                if fid not in filter_id_2_rounds:
                    filter_id_2_rounds[fid] = {
                        'previous_round': current_round,
                        'max_round': filters[fid]['rounds'],
                        'deduplication_id': 'round_' + str(id_inter),
                        'ids_timestamp': {}
                    }
                    if round_interactions is not None:
                        filter_id_2_rounds[fid]['deduplication_id'] = round_interactions['deduplication_id']
                    print(filter_id_2_rounds)
                filter_id_2_rounds[fid]['ids_timestamp'][id_inter] = interaction['timestamp_start']
    return messages, filter_id_2_rounds
