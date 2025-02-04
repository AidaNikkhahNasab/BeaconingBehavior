from influxdb_client import InfluxDBClient
import matplotlib.pyplot as plt
import pandas as pd
import json

# Load configuration from config.json
config_path = r"C:\Allianz\4\0211\1201\config.json"
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

url = config["url"]
token = config["token"]
org = config["org"]
bucket = config["bucket"]

try:
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

    # Extract hour from the timestamp
    df["hour"] = df["timestamp"].dt.hour

    # Group by URL and hour to count visits
    df_grouped = df.groupby(["url_hostname", "hour"]).size().reset_index(name="visit_count")

    # Filter out entries with less than 500 visits
    df_filtered = df_grouped[df_grouped["visit_count"] >= 500]

    # Plot: Full-Day Line Chart (Filtered URLs)
    plt.figure(figsize=(14, 7))
    for url in df_filtered["url_hostname"].unique():
        df_url = df_filtered[df_filtered["url_hostname"] == url]
        plt.plot(
            df_url["hour"], 
            df_url["visit_count"], 
            color="lightgreen",  # Same light-green color for all lines
            linewidth=1.0
        )
    plt.xticks(range(0, 25), [f"{hour}:00" for hour in range(0, 25)], rotation=45)
    plt.xlabel("Hour of the Day", fontsize=12)
    plt.ylabel("Number of Visits", fontsize=12)
    plt.title("Number of Visit URLs by Hour (Filtered: Visits >= 500)", fontsize=14)
    plt.tight_layout()
    plt.show()

    # Calculate averages
    day_avg = df_filtered[df_filtered["hour"].between(0, 4)]["visit_count"].mean()
    night_avg = df_filtered[df_filtered["hour"].between(4, 23)]["visit_count"].mean()

    # Plot: Two-Bar Chart for Averages
    plt.figure(figsize=(8, 6))
    plt.bar(
        ["00:00-04:00 (Day Average)", "04:00-24:00 (Night Average)"], 
        [day_avg, night_avg], 
        color=["skyblue", "orange"], 
        alpha=0.7
    )
    plt.ylabel("Average Number of Visits", fontsize=12)
    plt.title("Day vs. Night Average Visits", fontsize=14)
    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")
