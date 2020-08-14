
import json
import boto3
import config
import interaction_analyis
from datetime import datetime


def polling_queue():
    """
    example messages:
        - {"device_id": 123, "status": 1}
        - {"round": {'previous_round': 1, 'max_round': 2, 'deduplication_id': 'round_1696',
        'ids_timestamp': {'1696': 1586167911, '1509': 1586168119, '1554': 1586127477}}}
        - {"recurrent": {"device_ids": [123, 234, 345], "timestamp_min_unix": 1586127477}}
    :return:
    """

    print('polling queue')
    sqs_client = boto3.resource('sqs', region_name='us-west-1')

    queue_read = sqs_client.Queue(config.get_sqs_patients_url())
    queue_write = sqs_client.Queue(config.get_sqs_notifications_url())
    queue_dead_letters = sqs_client.Queue(config.get_sqs_dead_letter_url())

    while 1:
        print('MESSAGE')
        messages = queue_read.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=10)
        devices_infected = []
        devices_healed = []
        backup_messages_body = []
        rounds = []
        for message in messages:
            print('Message received: {0}'.format(message.body))

            body = json.loads(message.body)
            backup_messages_body.append(body)

            if 'device_id' in body and 'status' in body:
                if str(body['status']) == str(config.get_status_infected()):
                    devices_infected.append(str(body['device_id']))
                if str(body['status']) == str(config.get_status_healed()):
                    devices_healed.append(str(body['device_id']))
            elif 'round' in body:
                rounds.append(body['round'])
            elif 'recurrent' in body:
                print('found recurrent infected check')
                if 'device_ids' in body['recurrent'] and 'timestamp_min_unix' in body['recurrent']:
                    print('gonna elaborate immediately', body['recurrent'])
                    interaction_analyis.compute_messages(body['recurrent']['device_ids'], [], [], [],
                                                         queue_read, queue_write, queue_dead_letters,
                                                         last_timestamp_filter=datetime.fromtimestamp(body['recurrent']['timestamp_min_unix']))
            message.delete()

        interaction_analyis.compute_messages(devices_infected, devices_healed, backup_messages_body, rounds, queue_read, queue_write, queue_dead_letters)


def test():
    interaction_analyis.compute_messages([1509], [], [], [], None, None, None, debug=True)
    rounds = [{'previous_round': 1, 'max_round': 3, 'deduplication_id': 'round_1696',
               'ids_timestamp': {'1696': 1586167911, '1509': 1586168119, '1554': 1586127477}}]
    # interaction_analyis.compute_messages([], [], [], rounds, None, None, None, debug=True)
    rounds = [{'previous_round': 2, 'max_round': 3, 'deduplication_id': 'round_1696',
               'ids_timestamp': {'1558': 1586127477}}]
    # interaction_analyis.compute_messages([], [], [], rounds, None, None, None, debug=True)


if __name__ == '__main__':

    polling_queue()
    # test()
