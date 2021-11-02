import json
import boto3
import re

# CONFIG
######################################################
######################################################

# CONFIG de S3
BUCKET = 's3-bounce-review'
FICHERO_DATOS = 'bounce_list.csv'

######################################################
######################################################




def process_user_sqs(mail):
    emails = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", mail)
    #print (emails[0])
    return emails[0]


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

        #resp = sqs_client.delete_message_batch(
        #    QueueUrl=queue_url, Entries=entries
        #)

        #if len(resp['Successful']) != len(entries):
        #    raise RuntimeError(
        #        f"Failed to delete messages: entries={entries!r} resp={resp!r}"
        #    )


contador=0
LISTAMENSAJES=[]
for message in get_messages_from_queue('https://eu-west-1.queue.amazonaws.com/611720150677/SNS-Bounce-Review'):
    #print(json.dumps(message))
    contador=contador+1
    process_user_sqs(json.dumps(message))
    LISTAMENSAJES.append(process_user_sqs(json.dumps(message)))

ELEMENTOSUNICOS = []
for ORDENANDO in LISTAMENSAJES:
    if ORDENANDO not in ELEMENTOSUNICOS:
        ELEMENTOSUNICOS.append(ORDENANDO)

#print(ELEMENTOSUNICOS)
print("Total mensajes procesados: " + str(contador))

NEW_EMAIL_LIST=[]
OLDUSER=[]
file1 = open(FICHERO_DATOS, 'r')
for lineas in file1:
    OLDUSER.append(lineas.replace("\n","")) 
print (OLDUSER)
print ("###")
outfile  = open(FICHERO_DATOS, "a")
    
for lineasescribir in ELEMENTOSUNICOS:
    if lineasescribir not in OLDUSER:
        print (lineasescribir)
        outfile.write(str(lineasescribir))
        outfile.write('\n')

outfile.close()
