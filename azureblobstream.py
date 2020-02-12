import os
import uuid
import random
import json
import time
import datetime
import calendar
import shutil
import psutil
from multiprocessing import Process, Value
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# This script will generate JSON documents and upload to Azure Blob Storage.
# It will create concurrent threads based on the number of available cores.

# Set these variables
total_seconds = 30
local_file_path = 'C:\\filestage'
CONN_STR = '<my account connection string>'
CONTAINER = '<my account container>'

def send(blob_service_client, file_count):
    global timeout_start
    global total_seconds
    # Loop for set duration
    while time.time() < timeout_start + total_seconds:
        # Define file id, name and path
        id = str(uuid.uuid4())
        local_file_name = id + "_TIME_" + str(calendar.timegm(time.gmtime())) + ".json"
        upload_file_path = os.path.join(local_file_path, local_file_name)

        # Create file contents
        itemcat  = ['A', 'B', 'C']
        location  = ['Chicago', 'Redmond', 'Edina', 'omaha']
        eventlvl  = ['High', 'Medium', 'Low']
        eventcat  = ['Category 1', 'Category 2', 'Category 3', 'Category 4', 'Category 5']
        reading = {'id':id, 'itemcat': random.choice(itemcat), 'location': random.choice(location), 'records':[{'eventlvl': random.choice(eventlvl), 'eventcat': random.choice(eventcat), 'timestamp': str(datetime.datetime.utcnow())}]}
        s = json.dumps(reading)

        # Write text to local file
        file = open(upload_file_path, 'w')
        file.write(s)
        file.close()

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=local_file_name)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data)
        
        # Increment global counter
        file_count.value += 1

# Capture time at start of execution    
timeout_start = time.time()

if __name__ == '__main__':
    start_time = time.time()
    print("Startime:" + str(datetime.datetime.utcnow()))
    
    # Create the BlobServiceClient object which will be used to create a container client
    client = BlobServiceClient.from_connection_string(CONN_STR)

    # Create local file directory if not existing
    if not os.path.exists(local_file_path):
        os.makedirs(local_file_path)

    # Instantiate shared variable for file counting
    file_count = Value('i',0)

    # Determine number of threads from available cores. -1 to avoid maxing system.
    n_cpus = psutil.cpu_count()
    n_threads = n_cpus - 1
    print("{0} cores are available. We will spawn {1} thread(s).".format(n_cpus, n_threads))

    # Create threads based on number of available cores
    jobs = []
    for i in range(n_threads):
        p = Process(target=send, args=(client,file_count))
        jobs.append(p)
        p.start()

    # Stop threads
    for proc in jobs:
        proc.join()

    # Clean-up files and communicate script completion
    print("Runtime: {0} seconds. Files: {1}".format(time.time() - start_time, str(file_count.value)))
    print("Cleanup local directory and files " + local_file_path)
    shutil.rmtree(local_file_path)
    print("*Complete*")