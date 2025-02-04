from influxdb_client import InfluxDBClient
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from matplotlib import colormaps
import json

# Load configuration from a JSON file
def load_config(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

try:
    # Path to the configuration file
    config_path = r"C:\\Allianz\\4\\0211\\1201\\config.json"  # Use raw string for Windows paths
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
        # Restrict to the first 25 unique URLs
        unique_urls = df["url_hostname"].unique()[:25]
        df = df[df["url_hostname"].isin(unique_urls)]

        # Sort data by URL and timestamp
        df = df.sort_values(by=["url_hostname", "timestamp"])

        # Calculate time intervals between consecutive visits for each URL
        df["time_interval"] = df.groupby("url_hostname")["timestamp"].diff().dt.total_seconds()

        # Drop rows with NaN time intervals (first row for each URL)
        df_intervals = df.dropna(subset=["time_interval"]).copy()

        # Panel 1: 0-65 seconds (1-second bins)
        bins_seconds = np.arange(0, 66, 1)  # 0-65 seconds, 1-second bins
        labels_seconds = [f"{i}-{i+1}s" for i in range(65)]
        df_intervals["interval_bin_seconds"] = pd.cut(
            df_intervals["time_interval"], bins=bins_seconds, labels=labels_seconds, right=False
        )

        histogram_table_seconds = df_intervals.pivot_table(
            index="interval_bin_seconds", values="time_interval", aggfunc="count", fill_value=0, observed=False
        )

        # Generate the first panel
        cmap = colormaps.get_cmap("tab20")

        histogram_table_seconds.plot(
            kind="bar",
            stacked=True,
            figsize=(16, 8),
            logy=True,
            color=cmap(0.1)
        )
        plt.xlabel("Time Interval (Seconds)", fontsize=12)
        plt.ylabel("Count of Intervals (Log Scale)", fontsize=12)
        plt.title("Time Interval Histogram (0-65 Seconds)", fontsize=14)
        plt.xticks(ticks=np.arange(0, 65, 10), labels=[f"{i}s" for i in range(0, 66, 10)], rotation=45, ha="right", fontsize=10)
        plt.tight_layout()
        plt.show()

        # Panel 2: 1 minute to the rest of the bins
        bins_minutes = np.arange(1, 31) * 60  # 1min-30min, 1-min bins
        extended_bins = np.concatenate([bins_minutes, [float("inf")]])  # >30min as the last bin
        labels_minutes = [f"{i}min" for i in range(1, 31)] + [">30min"]
        df_intervals["interval_bin_minutes"] = pd.cut(
            df_intervals["time_interval"], bins=extended_bins, labels=labels_minutes, right=False
        )

        histogram_table_minutes = df_intervals.pivot_table(
            index="interval_bin_minutes", values="time_interval", aggfunc="count", fill_value=0, observed=False
        )

        # Generate the second panel
        histogram_table_minutes.plot(
            kind="bar",
            stacked=True,
            figsize=(16, 8),
            logy=True,
            color=cmap(0.1)
        )
        plt.xlabel("Time Interval (Minutes)", fontsize=12)
        plt.ylabel("Count of Intervals (Log Scale)", fontsize=12)
        plt.title("Time Interval Histogram (1 Minute to >30 Minutes)", fontsize=14)
        plt.xticks(rotation=45, ha="right", fontsize=10)
        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"An error occurred: {e}")