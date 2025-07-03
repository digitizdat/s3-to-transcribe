import json
import random
import urllib.parse
import boto3
import os

print('Loading function')

if 'OUTPUT_KEY' in os.environ:
    print('Using output key from environment variable')
    outputkey = os.environ['OUTPUT_KEY']
else:
    outputkey = 'transcriptions/'

if 'OUTPUT_BUCKET' in os.environ:
    print('Using output bucket from environment variable')
    outputbucket = os.environ['OUTPUT_BUCKET']
else:
    outputbucket = ''

if 'LANGUAGE_CODE' in os.environ:
    print('Using language code from environment variable')
    langcode = os.environ['LANGUAGE_CODE']
else:
    langcode = 'en-US'

if 'SHOW_SPEAKER_LABELS' in os.environ:
    print('Using show speaker labels from environment variable')
    show_speaker_labels = os.environ['SHOW_SPEAKER_LABELS']
    if show_speaker_labels.lower() == 'true':
        show_speaker_labels = True
    if show_speaker_labels.lower() == 'false':
        show_speaker_labels = False
        max_speaker_labels = 0
else:
    show_speaker_labels = True
    max_speaker_labels = 10

if 'MAX_SPEAKER_LABELS' in os.environ:
    print('Using max speaker labels from environment variable')
    max_speaker_labels = int(os.environ['MAX_SPEAKER_LABELS'])
else:
    if show_speaker_labels == True:
        max_speaker_labels = 10

settings = {'ShowSpeakerLabels': show_speaker_labels, 'MaxSpeakerLabels': int(max_speaker_labels)}

s3 = boto3.client('s3')
transcribe = boto3.client('transcribe')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    if outputkey == '':
        print('No output key specified in environment configuration')
        return False
    if outputbucket == '':
        print('No output bucket specified in environment configuration')
        return False

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    jobname = key[key.rindex('/')+1:-4]+'-'+str(random.randint(10000,99999))
    media = {'MediaFileUri': 's3://'+bucket+'/'+key}

    print('Starting transcription job '+jobname)
    r = transcribe.start_transcription_job(TranscriptionJobName=jobname, LanguageCode=langcode, Settings=settings, Media=media, OutputBucketName=outputbucket, OutputKey=outputkey)
    print(r)
    return r['ResponseMetadata']['HTTPStatusCode']
