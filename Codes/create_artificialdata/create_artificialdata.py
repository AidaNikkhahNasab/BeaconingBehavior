from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime
import random

# InfluxDB connection details
url = "http://localhost:8086"
token = "WUxftnono0_k_t620srsO7xNG15xcej5meoShrr1ONHGvWSEqwg3gJVhthKwux7wUyw1_1hm9TAQFWKeEBHK2g=="
org = "Student"
bucket = "Net1"

# Create InfluxDB client
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Generate beaconing data with multiple intervals
def generate_beaconing_data():
    url_hostname = "beaconing1@example.com"
    ip_address = "127.0.0.1"
    is_A = "yes"
    user = "-"
    
    # Start time for the first interval on 2023-08-01
    base_time = datetime.datetime(2023, 8, 1, 0, 0, 0)  # Start of day 2023-08-01
    
    # Intervals with visits between 1000 and 1500
    intervals = [random.randint(1000, 1500) for _ in range(10)]  # 10 intervals with random visits per interval

    data_points = []

    # Generate data points for each interval
    for interval in intervals:
        for _ in range(interval):  # Simulate multiple visits within each interval
            # Increase the timestamp with random intervals for each visit within the period
            base_time += datetime.timedelta(seconds=random.randint(1, 3))  # Randomize the time slightly
            timestamp_ns = int(base_time.timestamp() * 1e9)  # Convert to nanoseconds

            # Create a point for each visit
            point = Point("hostnames") \
                .tag("url_hostname", url_hostname) \
                .tag("ip_address", ip_address) \
                .tag("is_A", is_A) \
                .field("user", user) \
                .time(timestamp_ns)

            data_points.append(point)
    
    return data_points

# Write data to InfluxDB
def write_data_to_influx():
    data_points = generate_beaconing_data()
    write_api.write(bucket=bucket, org=org, record=data_points)
    print(f"Sample beaconing data inserted into InfluxDB: {len(data_points)} points")

# Run the script to insert the data
write_data_to_influx()

# Query data from InfluxDB to verify it was inserted
def query_data_from_influx():
    query = f"""
    from(bucket: "{bucket}")
      |> range(start: 2023-08-01T00:00:00Z, stop: 2023-08-02T00:00:00Z)
      |> filter(fn: (r) => r._measurement == "hostnames")
      |> filter(fn: (r) => r.url_hostname == "beaconing1@example.com")
      |> filter(fn: (r) => r.ip_address == "127.0.0.1")
      |> filter(fn: (r) => r.is_A == "yes")
      |> limit(n: 10)
    """
    result = client.query_api().query(query, org=org)
    
    if result:
        print("Query results:")
        for table in result:
            for record in table.records:
                print(record)
    else:
        print("No data found in the query!")

# Run the query function to verify data
query_data_from_influx()

# Close the client
client.close()
