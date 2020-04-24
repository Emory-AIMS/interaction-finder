# How it works

- The consumer is in charge to look for interacted devices and prepare the notification body to send to each device.
- The consumer make a polling on the `doctors infeced` queue.
- The consumer write notifications body in the `potential-infected-notification` queue.
- The consumer write input messages in the `doctor-infected-dead-letter` queue in case of exceptions.
- The body of the input queue has the following format: `{"device_id": 123, "status": 1}`.
- The body of the input queue has the following format: `{"round": {"previous_round": 1, "max_round": 2, "deduplication_id": "round_1696", "ids_timestamp": {"1696": 1586167911, "1509": 1586168119, "1554": 1586127477}}}` when looking at interactions after the first level of closeness.
- The body of the input queue has the following format: `{"recurrent": {"device_ids": [123, 234, 345], "timestamp_min_unix": 1586127477}}` when looking for interactions of already infected detected devices.
- Every update of status arrives here, any status different from `infected` (1) and `healed` (3) is ignored.
- Every infected or healed device in input will receive a notification with the status update.
- For every infected device we look for his interactions based on the filters set by virologists, the device will receive a notification.

### Periodic check

Periodically the system need to check new interaction for infected devices. 

This has been done in `periodic_check_infected.periodic_new_infected_interactions_check`

# Deployment instructions

## Requirements

- Python 3
- Pip 3
- [Boto 3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

## Step 1 - Download repository
Download repository
```bash
git clone https://gitlab.com/coronavirus-outbreak-control/interaction-finder
```

## Step 2 - Config.py
Edit following constants in `/config.py` file:
```
SQS_QUE_URL_PATIENTS = 'XXXXXXXXX'
SQS_QUE_URL_NOTIFICATIONS = 'XXXXXXXXX'
SQS_QUE_URL_DEAD_LETTERS = 'XXXXXXXXX'
S3_BUCKET_INTERACTION = 'XXXXXXXXX'

MYSQL_HOST = 'XXXXXXXXX'
MYSQL_PORT = XXXXXXXXX
MYSQL_DB_NAME = 'XXXXXXXXX'
MYSQL_USER = 'XXXXXXXXX'
MYSQL_PASSWORD = 'XXXXXXXXX'
```
Replace the `XXXXXXXXX` with your values.

## Step 3 - Setup service
Get the current user, type `# whoami`

Open `consumer-patients.service` and edit the `user` value with the name of the current user.

In order to set `consumer-patients.service` as systemctl service, create a symlink of our service
```bash
sudo ln -s ./consumer-patients.service /etc/systemd/system/consumer-patients.service
```
Start the service:
```bash
systemctl start consumer-patients.service
```
And automatically get it to start at boot:
```bash
systmctl enable consumer-patients.service
```
To test the system, simply type:
```bash
systemctl status consumer-patients.service
```
The output should be something like:
```bash
● consumer-patients.service - Consumer Patients.
   Loaded: loaded
   Active: active (running)
```
That's it.
