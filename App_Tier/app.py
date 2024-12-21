import os
import base64
import json
import subprocess
import boto3
import time
from face_recognition import face_match
from botocore.exceptions import ClientError

# AWS configuration
region_name = 'us-east-1'
asu_id = '1229407428'  # Replace with your actual ASU ID
req_queue_url = f'https://sqs.{region_name}.amazonaws.com/{asu_id}-req-queue'
resp_queue_url = f'https://sqs.{region_name}.amazonaws.com/{asu_id}-resp-queue'
input_bucket = f'{asu_id}-in-bucket'
output_bucket = f'{asu_id}-out-bucket'

# AWS clients
sqs = boto3.client('sqs', 
                   region_name=region_name, 
                   )
s3 = boto3.client('s3', 
                   region_name=region_name, 
                   )

# Paths
home_dir = os.path.expanduser('~')
model_dir = os.path.join(home_dir, 'model')
app_dir = os.path.join(home_dir, 'app')
temp_dir = os.path.join(app_dir, 'temp')

if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

def process_image(file_path):
    python_script_path = os.path.join(model_dir, 'face_recognition.py')
    result = subprocess.run(['python3', python_script_path, file_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error in face recognition: {result.stderr}")
    return result.stdout.strip()

def process_sqs_messages():
    try:
        response = sqs.receive_message(
            QueueUrl=req_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            for message in response['Messages']:
                message_id = message['MessageId']  # Get the Message ID from the SQS message
                body = json.loads(message['Body'])
                file_name = body['file_name']
                # image_data = body['imageData']
                image_path = os.path.join('/home/ubuntu/face_images_1000', file_name)
                result_image = face_match(image_path, 'data.pt')

                # Combine the Message_ID with the result_image in a dictionary
                result_payload = {
                    "Message_ID": message_id,
                    "Result_Image": result_image
                }

                # Upload the result_payload to S3
                try:
                    s3.put_object(
                        Bucket=output_bucket,
                        Key=f"{file_name.split('.')[0]}_result.json",  # Save as a JSON file in S3
                        Body=json.dumps(result_payload)
                    )
                    print(f"Uploaded result to S3: {file_name}")
                except ClientError as e:
                    print(f"Error uploading to S3: {e}")
                    continue

                # Send the combined (Message_ID, result_image) to the SQS queue
                try:
                    sqs.send_message(
                        QueueUrl=resp_queue_url,
                        MessageBody=json.dumps(result_payload)
                    )
                    print(f"Response from SQS send_message: {response}")
                    print(f"Sent result to SQS for message ID: {message_id}")
                except ClientError as e:
                    print(f"Error sending message to SQS: {e}")
                    continue

                # Delete the original message from the request queue
                try:
                    sqs.delete_message(
                        QueueUrl=req_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print(f"Deleted message from request queue: {message_id}")
                except ClientError as e:
                    print(f"Error deleting message from SQS: {e}")
                    continue

        else:
            print("No messages in queue.")
            time.sleep(1)

    except ClientError as e:
        print(f"Error processing request: {e}")

def main():
    print("Starting App Tier processing...")
    while True:
        process_sqs_messages()
        time.sleep(1)  # Short delay to avoid tight looping

if __name__ == '__main__':
    main()