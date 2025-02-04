import os
import json
from dateutil import parser
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.rest import ApiException

# InfluxDB connection details
url = "http://localhost:8086"
token = "WUxftnono0_k_t620srsO7xNG15xcej5meoShrr1ONHGvWSEqwg3gJVhthKwux7wUyw1_1hm9TAQFWKeEBHK2g=="
org = "Student"
bucket = "Net"

# Path to the folder containing the data files
folder_path = r"C:\Users\aydan\Downloads\export\EXPORT"

# Create an InfluxDB client
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

# Function to check if a record exists
def record_exists(bucket, org, logdate, url_hostname):
    try:
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -30d)  // Adjust the time range to match your dataset
          |> filter(fn: (r) => r["_measurement"] == "hostnames")
          |> filter(fn: (r) => r["url_hostname"] == "{url_hostname}")
          |> filter(fn: (r) => r["_time"] == {int(parser.parse(logdate).timestamp()) * 1_000_000_000})
        '''
        tables = query_api.query(org=org, query=query)
        return any(tables)
    except ApiException as e:
        print(f"Error querying InfluxDB: {e}")
        return False

# Function to update an existing record with the file name
def update_record(bucket, org, logdate, url_hostname, file_name):
    try:
        # Query for existing record
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "hostnames")
          |> filter(fn: (r) => r["url_hostname"] == "{url_hostname}")
        '''
        tables = query_api.query(org=org, query=query)
        if tables:
            print(f"Updating record for {url_hostname} with filename {file_name}")
            # Logic to update record with filename (e.g., use a specific update query or append new data)
        return True
    except ApiException as e:
        print(f"Error updating InfluxDB: {e}")
        return False

# Function to process a single file
def process_file(file_path, write_api):
    file_name = os.path.basename(file_path)
    try:
        with open(file_path, "r") as file:
            for line in file:
                try:
                    entry = json.loads(line.strip())
                    if "logdate" in entry and "url_hostname" in entry and "user" in entry:
                        logdate = entry["logdate"]
                        url_hostname = entry["url_hostname"]
                        timestamp_ns = int(parser.parse(logdate).timestamp()) * 1_000_000_000

                        # Check if the record exists
                        if record_exists(bucket, org, logdate, url_hostname):
                            update_record(bucket, org, logdate, url_hostname, file_name)
                        else:
                            print(f"Inserting new record for {url_hostname} at {logdate}")

                            # Create a point
                            point = Point("hostnames") \
                                .tag("url_hostname", url_hostname) \
                                .tag("IP_address", file_name) \
                                .field("user", entry["user"]) \
                                .time(timestamp_ns)

                            # Write the point
                            write_api.write(bucket=bucket, record=point)
                    else:
                        print(f"Invalid entry in file {file_path}: {entry}")
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON line in file {file_path}: {e}")
                except Exception as e:
                    print(f"Error processing line in file {file_path}: {e}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

# Iterate over all files in the directory and process files
with client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=10_000)) as write_api:
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):  # Ensure it is a file
            print(f"Processing file: {file_name}")
            process_file(file_path, write_api)

print("Data import completed.")
