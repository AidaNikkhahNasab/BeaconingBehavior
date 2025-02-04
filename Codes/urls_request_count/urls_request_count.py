import json
from influxdb_client import InfluxDBClient
import matplotlib.pyplot as plt
import pandas as pd

# Load configuration from a JSON file
def load_config(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

try:
    # Path to the configuration file
    config_path = r"C:\Allianz\4\0211\1201\config.json"  # Use raw string for Windows paths
    config = load_config(config_path)

    # Extract InfluxDB connection details from the config
    url = config["url"]
    token = config["token"]
    org = config["org"]
    bucket = config["bucket"]

    # Create an InfluxDB client
    client = InfluxDBClient(url=url, token=token, org=org)

    # Query data from InfluxDB
    query = f'from(bucket:"{bucket}") |> range(start: 2023-08-01T00:00:00Z, stop: 2023-08-02T00:00:00Z)'
    tables = client.query_api().query(query, org=org)

    # Extract points from the result
    extracted_influx_objects = {}
    for table in tables:
        for record in table.records:
            url_hostname = record.values.get("url_hostname")
            if url_hostname:
                extracted_influx_objects[url_hostname] = extracted_influx_objects.get(url_hostname, 0) + 1

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(list(extracted_influx_objects.items()), columns=["URL Hostname", "Request Count"])

    # Filter URLs with more than 500 visits
    df_filtered = df[df["Request Count"] > 500]

    # Sort the filtered data by "Request Count" in descending order
    df_filtered = df_filtered.sort_values(by="Request Count", ascending=False)

    # Save filtered data to a CSV file
    output_path = r"C:\Allianz\4\0211\1201\filtered_urls.csv"
    df_filtered.to_csv(output_path, index=False)
    print(f"Filtered data saved to {output_path}")

    # Plot the histogram for filtered URLs
    plt.figure(figsize=(14, 7))
    bars = plt.bar(range(1, len(df_filtered) + 1), df_filtered["Request Count"], color="skyblue")  # Numeric labels

    # Remove x-axis labels (empty)
    plt.xticks(range(1, len(df_filtered) + 1), [''] * len(df_filtered))

    # Adding labels and title
    plt.xlabel("URL Hostname Index", fontsize=12)
    plt.ylabel("Request Counts", fontsize=12)  # Normal linear scale
    plt.title("Request Count", fontsize=14)

    # Layout and show plot
    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")
