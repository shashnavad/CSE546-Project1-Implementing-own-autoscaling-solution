from flask import Flask, request, Response
import boto3
import json
import base64
import threading
import time
from botocore.exceptions import ClientError
import sys

app = Flask(__name__)

# AWS configuration
region_name = 'us-east-1'
asu_id = '1229407428'
req_queue_url = f'https://sqs.{region_name}.amazonaws.com/{asu_id}-req-queue'
resp_queue_url = f'https://sqs.{region_name}.amazonaws.com/{asu_id}-resp-queue'
input_bucket = f'{asu_id}-in-bucket'
output_bucket = f'{asu_id}-out-bucket'
APP_TIER_AMI_ID = 'ami-0bcb2ba173e7e10b2'

# AWS clients
sqs = boto3.client('sqs', 
                   region_name=region_name, 
                   )
s3 = boto3.client('s3', 
                   region_name=region_name, 
                   )
ec2 = boto3.client('ec2', 
                   region_name=region_name, 
                   )

pending_classifications = {}

def get_app_tier_instance_ids():
    try:
        print("Fetching app tier instance IDs...")
        response = ec2.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': ['app-tier-instance-*']
                },
                {
                    'Name': 'instance-state-name',
                    'Values': ['running', 'stopped', 'pending', 'stopping']
                }
            ]
        )
        
        instance_ids = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_ids.append(instance['InstanceId'])
        
        print(f"Fetched instance IDs: {instance_ids}")
        return instance_ids
    except ClientError as e:
        print(f"Error fetching instance IDs: {e}")
        return []

def rename_instances(new_instance_ids, current_instance_count):
    for i, instance_id in enumerate(new_instance_ids):
        for attempt in range(3):  # Retry up to 3 times
            try:
                print(f"Waiting for instance {instance_id} to be running...")
                ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])

                # Tag the instance with a unique name
                ec2.create_tags(
                    Resources=[instance_id],
                    Tags=[
                        {
                            'Key': 'Name',
                            'Value': f'app-tier-instance-{current_instance_count + i + 1}'
                        }
                    ]
                )
                print(f"Successfully renamed instance: {instance_id} to app-tier-instance-{current_instance_count + i + 1}")
                break  # Exit retry loop on success
            except ClientError as e:
                print(f"Error tagging instance {instance_id} (attempt {attempt+1}): {e}")
                time.sleep(5)  # Wait before retrying
        else:
            print(f"Failed to rename instance: {instance_id} after 3 attempts")

def adjust_ec2_instances(target_running_instances):
    print(f"Adjusting EC2 instances to meet target of {target_running_instances} running instances...")
    instance_ids = get_app_tier_instance_ids()
    response = ec2.describe_instances(InstanceIds=instance_ids)
    running_instances = []
    stopped_instances = []

    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            instance_name = next((tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'), None)

            if instance_name and instance_name.startswith('app-tier-instance-'):
                state = instance['State']['Name']
                if state in ['running', 'pending']:
                    running_instances.append(instance['InstanceId'])
                elif state == 'stopped':
                    stopped_instances.append(instance['InstanceId'])

    running = len(running_instances)
    inst_needed = target_running_instances - running

    print(f"Currently running instances: {running}, Instances needed: {inst_needed}")

    if running >= target_running_instances:
        print(f"Current instances ({running}) meet or exceed target ({target_running_instances}). No action needed.")

    if inst_needed > 0:
        instances_to_start = stopped_instances[:inst_needed]
        if instances_to_start:
            try:
                ec2.start_instances(InstanceIds=instances_to_start)
                print(f"Started instances: {', '.join(instances_to_start)}")
            except ClientError as e:
                print(f"Error starting instances: {e}")
        
        new_instances_needed = inst_needed - len(instances_to_start)
        if new_instances_needed > 0:
            try:
                current_instance_count = len(instance_ids)
                
                tag_specifications = [
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {
                                'Key': 'Name',
                                'Value': 'app-tier-instance'
                            } 
                        ]
                    } 
                ]

                print(f"Launching {new_instances_needed} new instances...")
                response = ec2.run_instances(
                    ImageId=APP_TIER_AMI_ID,  
                    InstanceType='t2.micro',  
                    MinCount=new_instances_needed,
                    MaxCount=new_instances_needed,
                    TagSpecifications=tag_specifications,
                )
                new_instance_ids = [instance['InstanceId'] for instance in response['Instances']]
                print(f"Launched new instances: {', '.join(new_instance_ids)}")

                rename_instances(new_instance_ids, current_instance_count)

    

            except ClientError as e:
                print(f"Error launching new instances: {e}")

    elif inst_needed < 0:
        '''# Determine the instances to stop (select the last instances to minimize disruption)
        print(f"Running instances: {running_instances}")

        # Filter instances that match the desired prefix
        instances_to_stop = [
            instance_id for instance_id in running_instances
            if instance_id.startswith('app-tier-instance-')
        ]
        
        print(f"Filtered instances to stop: {instances_to_stop}")'''
        
        instances_to_terminate = running_instances[target_running_instances:]
        if instances_to_terminate:
            try:
                print(f"Stopping instances: {', '.join(instances_to_terminate)}")
                ec2.terminate_instances(InstanceIds=instances_to_terminate)
                print(f"Stopped instances: {', '.join(instances_to_terminate)}")
            except ClientError as e:
                print(f"Error stopping instances: {e}")
        else:
            print("No instances to stop")


def fetch_message_count():
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=req_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        message_count = int(response['Attributes']['ApproximateNumberOfMessages'])
        print(f"Fetched message count: {message_count}")
        return message_count
    except ClientError as e:
        print(f"Error fetching message count: {e}")
        return 0

def adjust_instance_count():
    while not stop_event.is_set():
        messages = fetch_message_count()
        print(f"Current message count: {messages}")

        if messages == 0:
            time.sleep(4)
            messages = fetch_message_count()
            print(f"Recheck count of requests: {messages}")

        target_instances = min(20, messages if messages <= 10 else 10 + ((messages - 10) + 3) // 4)
        adjust_ec2_instances(target_instances)
        time.sleep(20)

def fetch_and_handle_queue_messages(req_message_id):
    while not stop_event.is_set():
        try:
            response = sqs.receive_message(
                QueueUrl=resp_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )

            if 'Messages' in response:
                for message in response['Messages']:
                    
                    body = json.loads(message['Body'])
                    message_id = body.get('Message_ID')
                    result_image = body.get('Result_Image')
                    
                    if message_id == req_message_id:
                        if message_id in pending_classifications:
                            pending_classifications[message_id].set()
                        try:
                            sqs.delete_message(
                                QueueUrl=resp_queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            print(f"Deleted message from SQS: {message_id}")
                        except ClientError as e:
                            if e.response['Error']['Code'] == 'ReceiptHandleIsInvalid':
                                print(f"Message already deleted from SQS: {message_id}")
                            else:
                                raise
                        return result_image
        except ClientError as e:
            print(f"Error handling SQS messages: {e}")


@app.route('/', methods=['POST'])
def process_image():
    if 'inputFile' not in request.files:
        print("No file uploaded")
        return Response('No file uploaded', status=400)

    file = request.files['inputFile']
    if file.filename == '':
        print("No selected file")
        return Response('No selected file', status=400)

    if file:
        file_name = file.filename
        file_content = file.read()

        print(f"Uploading file to S3: {file_name}")
        try:
            s3.put_object(Bucket=input_bucket, Key=file_name, Body=file_content)
            print(f"File uploaded successfully: {file_name}")
        except ClientError as e:
            print(f"Error uploading file to S3: {e}")
            return Response('Error uploading file', status=500)

        # Send message to SQS
        message_body = json.dumps({"file_name": file_name})
        try:
            response = sqs.send_message(QueueUrl=req_queue_url, MessageBody=message_body)
            message_id = response['MessageId']
            result = fetch_and_handle_queue_messages(message_id)
            print(f"Message sent to SQS for file: {file_name}")
        except ClientError as e:
            print(f"Error sending message to SQS: {e}")
            return Response('Error sending message', status=500)

        return f'{file_name}:{result[0]}'

stop_event = threading.Event()

def start_background_threads():
    print("Starting background threads...", flush=True)
    thread1 = threading.Thread(target=adjust_instance_count)
    thread1.start()

    print("Background threads started", flush=True)

if __name__ == '__main__':
    # Start background threads
    start_background_threads()

    try:
        # Start Flask app
        app.run(host='0.0.0.0', port=8000)
    finally:
        # Set stop event to stop threads
        stop_event.set()