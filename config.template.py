import sys

##################
# AWS Parameters #
##################

# AWS SQS queue url where infected masked-ID are sent
SQS_QUE_URL_PATIENTS = ''
# AWS SQS queue url for notifications
SQS_QUE_URL_NOTIFICATIONS = ''
# AWS SQS dead letter queue url for notifications
SQS_QUE_URL_DEAD_LETTERS = ''
# AWS S3 bucket name where interactions are stored
S3_BUCKET_INTERACTION = ''

####################
# MySql Parameters #
####################

# MySql host
MYSQL_HOST = ''
# MySql port (set default as 3306)
MYSQL_PORT = 0
# MySql database name
MYSQL_DB_NAME = ''
# MySql username
MYSQL_USER = ''
# MySql password
MYSQL_PASSWORD = ''




def get_s3_bucket_name():
    return S3_BUCKET_INTERACTION


def get_sqs_patients_url():
    return SQS_QUE_URL_PATIENTS


def get_sqs_notifications_url():
    return SQS_QUE_URL_NOTIFICATIONS

def get_sqs_dead_letter_url():
    return SQS_QUE_URL_DEAD_LETTERS

def get_db_user():
    if len(sys.argv) > 1:
        return sys.argv[1]
    return MYSQL_USER


def get_db_password():
    if len(sys.argv) > 2:
        return sys.argv[2]
    return MYSQL_PASSWORD


def get_db_host():
    return MYSQL_HOST


def get_db_port():
    return MYSQL_PORT


def get_db_name():
    return MYSQL_DB_NAME


def get_timestamp_format_string():
    return '%Y-%m-%dT%H:%M:%S'


def days_look_back():
    return 14


def get_status_infected():
    return 1


def get_status_healed():
    return 3


def get_number_processes():
    return 8
