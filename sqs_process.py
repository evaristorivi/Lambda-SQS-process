# -*- coding: utf-8 -*-

# Lambda-SQS-process
# Subscribe an SQS queue to an SNS topic and this code will export the emails found and add them to a csv in S3.

#Evaristo R. Rivieccio Vega 



import json
import boto3
import re

# CONFIG
######################################################
######################################################

# CONFIG de S3
BUCKET = 'BUCKET'
DATA_FILE = 'FILE.csv'

######################################################
######################################################

# CONFIG SQS
URL_QUEUE = 'URL_SQS'


USUARIOS_A_MON = [
    "['USUARIOS_A_FILTRAR']",
    "['USUARIOS_A_FILTRAR']"
    ]


def get_messages_from_queue(queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """
    sqs_client = boto3.client('sqs')

    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10
        )

        try:
            yield from resp['Messages']
        except KeyError:
            return

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        # resp = sqs_client.delete_message_batch(
        #     QueueUrl=queue_url, Entries=entries
        # )

        # if len(resp['Successful']) != len(entries):
        #     raise RuntimeError(
        #         f"Failed to delete messages: entries={entries!r} resp={resp!r}"
        #     )

def lambda_handler(event, context):
    # Cliente s3
    s3 = boto3.resource('s3')
    s3_object = s3.Object(BUCKET, DATA_FILE)
    
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    counter=0
    LISTOFMESSAGES=[]
    for message in get_messages_from_queue(URL_QUEUE):
        content=json.loads(message["Body"])["Message"]
        if not content == 'Successfully validated SNS topic for Amazon SES event publishing.':
            counter=counter+1
            content_json=json.loads(content)
            caller_identity=str(content_json["mail"]["tags"]["ses:caller-identity"])
            if caller_identity in USUARIOS_A_MON:
                source=str(content_json["mail"]["source"])
                destination=str(content_json["mail"]["destination"])
                source_IP=str(content_json["mail"]["tags"]["ses:source-ip"])
                LISTOFMESSAGES.append("Caller_identity: " + caller_identity + " con IP: " + source_IP + " ha enviado desde: " + source + " a: " + destination)
    
    UNIQUEELEMENTS = []
    for ORDERING in LISTOFMESSAGES:
        if ORDERING not in UNIQUEELEMENTS:
            UNIQUEELEMENTS.append(ORDERING)

    #Test
    # for a in UNIQUEELEMENTS:
    #     print (a)

    print("Total messages processed: " + str(counter))
    outfile  = open('/tmp/' + DATA_FILE, "w")
    for recons in data:
        outfile.write(str(recons))
        outfile.write('\n')
    outfile.close()
    if counter == 0:
        print ("There are no new messages in the queue to be processed")
        exit()

    NEW_EMAIL_LIST=[]
    OLDUSER=[]
    #file1 = open(DATA_FILE, 'r')
    for lineas in data:
        OLDUSER.append(lineas)
    print ("The following users have already been added") 
    for b in OLDUSER:
        print (b)
    print ("###")
    outfile  = open('/tmp/' + DATA_FILE, "a")
    print ("The following users are added: ")
    for linewrite in UNIQUEELEMENTS:
        if linewrite not in OLDUSER:
            print (linewrite)
            outfile.write(str(linewrite))
            outfile.write('\n')

    outfile.close()

    # Upload to S3
    s3.Bucket(BUCKET).upload_file('/tmp/' + DATA_FILE, DATA_FILE)


    return {
        'statusCode': 200,
        'body': "Lambda ejecutada correctamente."
    }