from influxdb_client import InfluxDBClient
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import butter, filtfilt

# Function to calculate request power
def calculate_request_power(request_list):
    power_dictionary = {}
    last_date_time = request_list[0]["_time"]

    for request_dict in request_list:
        current_date_time = request_dict["_time"]

        # Check if current_date_time is a string, and convert it to a datetime object
        if isinstance(current_date_time, str):
            current_date_time = datetime.strptime(current_date_time, "%Y-%m-%dT%H:%M:%S.%fZ")

        time_delta = int((current_date_time - last_date_time).total_seconds())

        # Add the power to the power dictionary
        power_dictionary[time_delta] = power_dictionary.get(time_delta, 0) + 1
        last_date_time = current_date_time

    # Sort the dictionary for better visualization
    power_dictionary = dict(sorted(power_dictionary.items()))

    return power_dictionary

# Function to apply bandpass filtering in terms of time
def bandpass_filter(data, lowcut_time, highcut_time, sampling_rate, order=4):
    nyquist = 0.5 * sampling_rate
    lowcut = lowcut_time / nyquist
    highcut = highcut_time / nyquist

    if lowcut >= 1 or highcut >= 1:
        raise ValueError("Digital filter critical frequencies must be 0 < Wn < 1")

    b, a = butter(order, [lowcut, highcut], btype='band')
    filtered_data = filtfilt(b, a, data)
    return filtered_data

# InfluxDB connection details
url = "http://localhost:8086"
token = "WUxftnono0_k_t620srsO7xNG15xcej5meoShrr1ONHGvWSEqwg3gJVhthKwux7wUyw1_1hm9TAQFWKeEBHK2g=="
org = "Student"
bucket = "Net"
influx_username = 'aida'
influx_password = 'Niki7976'

try:
    # Create an InfluxDB client
    client = InfluxDBClient(url=url, token=token, org=org)

    # Query data from InfluxDB
    query = f'from(bucket:"{bucket}") |> range(start: 2023-08-01T00:00:00Z, stop: 2023-08-02T00:00:00Z)'
    tables = client.query_api().query(query, org=org)

    # Extract points from the result
    points = [record.values for table in tables for record in table.records]

    # Process and organize the InfluxDB data
    print("Processing InfluxDB Data:")
    extracted_influx_objects = {}

    for point in points:
        url_hostname = point.get("url_hostname")

        # Check if url_hostname is already in the dictionary
        if url_hostname not in extracted_influx_objects:
            extracted_influx_objects[url_hostname] = []

        # Append under the corresponding url_hostname
        extracted_influx_objects[url_hostname].append(point)

        # Print the extracted InfluxDB data for debugging
        print(point)

    # Create a whitelist to filter out unwanted URLs
    def create_whitelist(data, exclusion_criteria):
        whitelist = {url: requests for url, requests in data.items() if not any(exclusion in url for exclusion in exclusion_criteria)}
        return whitelist

    # Define exclusion criteria for URLs
    exclusion_criteria = ['allianz', 'res']

    # Create a whitelist based on the exclusion criteria
    whitelist = create_whitelist(extracted_influx_objects, exclusion_criteria)

    print("Filtered URLs based on whitelist:")
    for url_hostname in whitelist:
        print(url_hostname)

    # Create a table of power for each URL hostname with bandpass filtering in terms of time
    print("\nPower Table with Bandpass Filtering in Terms of Time:")
    for url_hostname, requests in whitelist.items():
        print(f"URL Hostname: {url_hostname}")
        power_dictionary = calculate_request_power(requests)

        # Print the power dictionary for debugging
        print("Power Dictionary:", power_dictionary)

        # Extract keys and values from the power dictionary
        time_intervals = list(power_dictionary.keys())
        power_values = list(power_dictionary.values())

        # Apply bandpass filtering in terms of time
        lowcut_time = 5  # 5 seconds
        highcut_time = 1000  # 1000 seconds

        # Check if there are enough elements to calculate sampling rate
        if len(time_intervals) >= 2:
            sampling_rate = 1.0 / (time_intervals[1] - time_intervals[0])  # Sampling frequency

            try:
                filtered_power_values = bandpass_filter(power_values, lowcut_time, highcut_time, sampling_rate)
            except ValueError as e:
                print(f"Error: {e}")
                filtered_power_values = power_values

            # Print the filtered power values for debugging
            print("Filtered Power Values:", filtered_power_values)

            # Calculate the average power
            average_power = sum(filtered_power_values) / len(filtered_power_values)

            # Print the average power for debugging
            print("Average Power:", average_power)

            # Subtract average power from all power values
            adjusted_power_values = [power - average_power for power in filtered_power_values]

            # Print the adjusted power values for debugging
            print("Adjusted Power Values:", adjusted_power_values)

            # Remove negative powers
            non_negative_power_values = [max(0, power) for power in adjusted_power_values]

            # Get indices for the time range of interest (5 to 1000 seconds)
            time_range_indices = [i for i, t in enumerate(time_intervals) if 5 <= t <= 1000]

            # Print the time range indices for debugging
            print("Time Range Indices:", time_range_indices)

            # Plot the adjusted data within the specified time range
            plt.plot([time_intervals[i] for i in time_range_indices], [non_negative_power_values[i] for i in time_range_indices], label=url_hostname)

    # Check if there are multiple URLs in the whitelist before creating legend
    if len(whitelist) > 1:
        plt.legend()

    plt.xlabel("Time Interval (seconds)")  # Change x-axis label
    plt.ylabel("Adjusted Power")  # Change y-axis label
    plt.title("Adjusted Power over Time")  # Change the chart title
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")
