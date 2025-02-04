from influxdb_client import InfluxDBClient
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from matplotlib.cm import get_cmap
import json

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
    extracted_data = []
    for table in tables:
        for record in table.records:
            url_hostname = record.values.get("url_hostname")
            timestamp = record.get_time()  # Timestamp of the record
            if url_hostname and timestamp:
                extracted_data.append({"url_hostname": url_hostname, "timestamp": pd.to_datetime(timestamp)})

    # Convert to DataFrame
    df = pd.DataFrame(extracted_data)

    if df.empty:
        print("No data retrieved from the query.")
    else:
        # Sort data by URL and timestamp
        df = df.sort_values(by=["url_hostname", "timestamp"])

        # Calculate time intervals between consecutive visits for each URL
        df["time_interval"] = df.groupby("url_hostname")["timestamp"].diff().dt.total_seconds()

        # Drop rows with NaN time intervals (first row for each URL)
        df_intervals = df.dropna(subset=["time_interval"])

        # Define bins for 0-60 seconds (1-second bins) and extended bins for minutes
        bins_seconds = np.arange(0, 61, 1)  # 0-60 seconds, 1-second bins
        bins_minutes = np.arange(1.5, 31.5, 1) * 60  # 1.5min-30.5min, 1-min bins
        bins = np.concatenate([bins_seconds, bins_minutes, [float("inf")]])  # >30min as the last bin
        labels = (
            [f"{i}-{i+1}s" for i in range(60)] +
            [f"{int(i/60)}min" for i in bins_minutes] +
            [">30min"]
        )

        # Assign bins
        df_intervals["interval_bin"] = pd.cut(
            df_intervals["time_interval"], bins=bins, labels=labels, right=False
        )

        # Create a histogram table
        histogram_table = df_intervals.groupby("interval_bin").size()

        # Save histogram table to CSV
        histogram_table.to_csv(output_path)
        print(f"Histogram table saved to {output_path}")

        # Generate plots
        cmap = get_cmap("tab20", len(labels))  # Generate a colormap
        colors = [cmap(i) for i in range(len(labels))]

        # Linear Scale Plot
        plt.figure(figsize=(16, 8))
        histogram_table.plot(kind="bar", color=colors, alpha=0.8, width=0.7)
        plt.xlabel("Time Interval", fontsize=12)
        plt.ylabel("Count of Intervals (Linear Scale)", fontsize=12)
        plt.title("Time Interval Histogram (Linear Scale)", fontsize=14)
        plt.xticks(rotation=45, ha="right", fontsize=10)  # Rotate x-axis labels for clarity
        plt.tight_layout()
        plt.show()

        # Logarithmic Scale Plot
        plt.figure(figsize=(16, 8))
        histogram_table.plot(kind="bar", color=colors, alpha=0.8, width=0.7)
        plt.yscale("log")
        plt.xlabel("Time Interval", fontsize=12)
        plt.ylabel("Count of Intervals (Logarithmic Scale)", fontsize=12)
        plt.title("Time Interval Histogram (Logarithmic Scale)", fontsize=14)
        plt.xticks(rotation=45, ha="right", fontsize=10)  # Rotate x-axis labels for clarity
        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"An error occurred: {e}")
