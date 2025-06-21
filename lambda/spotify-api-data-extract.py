import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):

    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    playlist_link = 'https://open.spotify.com/playlist/2VHPR4LikWWZqJ9KiJqRMv'
    playlist_uri = playlist_link.split('/')[-1]
    spotify_data = sp.playlist_tracks(playlist_uri)

    filename = 'spotify-raw-' + str(datetime.now()) + '.json'

    client = boto3.client('s3')
    client.put_object(
        Bucket='spotify-etl-project-ubaid',
        Key='raw-data/files-to-be-processed/'+filename,
        Body=json.dumps(spotify_data)
    )

