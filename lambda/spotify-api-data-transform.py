import boto3
import json
import pandas as pd
from io import StringIO
import os
from datetime import datetime

s3 = boto3.client('s3')

# S3 paths
BUCKET_NAME = 'spotify-etl-project-ubaid'  # ← change this
INPUT_PREFIX = 'raw-data/files-to-be-processed/'
PROCESSED_PREFIX = 'raw-data/files-processed/'
SONGS_OUTPUT_PREFIX = 'transformed-data/songs-data/'
ARTISTS_OUTPUT_PREFIX = 'transformed-data/artists-data/'
LOG_PREFIX = 'logs-data/'



def lambda_handler(event, context):
    log_entries = []  # To collect log messages

    # Timestamp for log filename
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
    log_filename = f'lambda-log-{timestamp}.txt'
    log_key = f'{LOG_PREFIX}{log_filename}'

    # List JSON files to process
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INPUT_PREFIX)
    if 'Contents' not in response:
        log_entries.append("No files to process.")
    else:
        for obj in response['Contents']:
            key = obj['Key']

            # Skip non-JSON files
            if not key.endswith('.json'):
                continue

            try:
                # Read JSON file
                file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                content = file_obj['Body'].read().decode('utf-8')
                Data = json.loads(content)

                json_filename = os.path.basename(key)
                timestamp_part = json_filename.replace('.json', '').split('raw-')[-1]

                # --- Song data ---
                songs_list = []
                for row in Data['items']:
                    for row2 in row['track']['artists']:
                        songs_dict = {
                            'song_id': row['track']['id'],
                            'song_name': row['track']['name'],
                            'release_date': row['track']['album']['release_date'],
                            'artist_name': row2['name'],
                            'artist_id': row2['id'],
                            'duration_ms': row['track']['duration_ms'],
                            'popularity': row['track']['popularity']
                        }
                        songs_list.append(songs_dict)

                songs_df = pd.DataFrame(songs_list)
                songs_buffer = StringIO()
                songs_df.to_csv(songs_buffer, index=False)

                songs_filename = f"songs-data-{timestamp_part}.csv"
                songs_output_key = f'{SONGS_OUTPUT_PREFIX}{songs_filename}'
                s3.put_object(Bucket=BUCKET_NAME, Key=songs_output_key, Body=songs_buffer.getvalue())

                # --- Artist data ---
                artist_list = []
                for row in Data['items']:
                    for row2 in row['track']['artists']:
                        artist_dict = {
                            'artist_id': row2['id'],
                            'artist_name': row2['name'],
                            'artist_uri': row2['uri'].split(":")[-1]
                        }
                        artist_list.append(artist_dict)

                artist_df = pd.DataFrame(artist_list).drop_duplicates(subset='artist_id')
                artist_buffer = StringIO()
                artist_df.to_csv(artist_buffer, index=False)

                artist_filename = f"artists-data-{timestamp_part}.csv"
                artist_output_key = f'{ARTISTS_OUTPUT_PREFIX}{artist_filename}'
                s3.put_object(Bucket=BUCKET_NAME, Key=artist_output_key, Body=artist_buffer.getvalue())

                # Move input to processed
                processed_key = key.replace(INPUT_PREFIX, PROCESSED_PREFIX)
                s3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': key}, Key=processed_key)
                s3.delete_object(Bucket=BUCKET_NAME, Key=key)

                # Log success
                log_entries.append(f"  → Processed file: {json_filename}")
                log_entries.append(f"  → Songs CSV: {songs_output_key}")
                log_entries.append(f"  → Artist CSV: {artist_output_key}")
                log_entries.append(f"  → Archived JSON: {processed_key}")

            except Exception as e:
                log_entries.append(f"  → Failed to process file: {key}")
                log_entries.append(f"  → Error: {str(e)}")

    # Save log file to S3
    log_buffer = StringIO()
    log_buffer.write('\n'.join(log_entries))
    s3.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=log_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': f'Lambda completed. Log written to {log_key}'
    }

