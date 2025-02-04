import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import butter, filtfilt

# Function to calculate request occurrence
def calculate_request_occurrence(request_list):
    occurrence_dictionary = {}
    last_date_time = None

    for _, request_row in request_list.iterrows():
        try:
            current_date_time = pd.to_datetime(request_row["_time"])

            if last_date_time is not None:
                time_delta = int((current_date_time - last_date_time).total_seconds())

                # Add the occurrence to the occurrence dictionary
                occurrence_dictionary[time_delta] = occurrence_dictionary.get(time_delta, 0) + 1

            last_date_time = current_date_time

        except (ValueError, TypeError) as e:
            print(f"Error in row: {_}, Timestamp value: {request_row['_time']}, Error message: {e}")

    # Sort the dictionary for better visualization
    occurrence_dictionary = dict(sorted(occurrence_dictionary.items()))

    return occurrence_dictionary

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

# CSV file path for the cleaned and modified data
csv_file_path = r'C:\Allianz\4\1125\Modified_Beaconing.csv'

try:
    # Read data from CSV file and explicitly convert "_time" to datetime
    df = pd.read_csv(csv_file_path)
    df["_time"] = pd.to_datetime(df["_time"], format='%H:%M:%S.%f', errors='coerce')

    # Process and organize the CSV data
    print("Processing CSV Data:")
    extracted_csv_objects = {}

    for _, row in df.iterrows():
        url_hostname = row.get("url_hostname")

        # Check if url_hostname is already in the dictionary
        if url_hostname not in extracted_csv_objects:
            extracted_csv_objects[url_hostname] = []

        # Append under the corresponding url_hostname
        extracted_csv_objects[url_hostname].append(row)

        # Print the extracted CSV data for debugging
        print(row)

    # Create a whitelist to filter out unwanted URLs
    def create_whitelist(data, exclusion_criteria):
        whitelist = {url: requests for url, requests in data.items() if not any(exclusion in url for exclusion in exclusion_criteria)}
        return whitelist

    # Define exclusion criteria for URLs
    exclusion_criteria = ['allianz', 'res']

    # Create a whitelist based on the exclusion criteria
    whitelist = create_whitelist(extracted_csv_objects, exclusion_criteria)

    print("Filtered URLs based on whitelist:")
    for url_hostname in whitelist:
        print(url_hostname)

    # Create a table of occurrence for each URL hostname with bandpass filtering in terms of time
    print("\nOccurrence Table with Bandpass Filtering in Terms of Time:")
    peak_url_hostname = None
    peak_occurrence_value = 0
    for url_hostname, requests in whitelist.items():
        print(f"URL Hostname: {url_hostname}")
        occurrence_dictionary = calculate_request_occurrence(pd.DataFrame(requests))

        # Print the occurrence dictionary for debugging
        print("Occurrence Dictionary:", occurrence_dictionary)

        # Identify peak occurrence value and corresponding URL hostname
        if occurrence_dictionary:
            max_occurrence_value = max(occurrence_dictionary.values())
            if max_occurrence_value > peak_occurrence_value:
                peak_occurrence_value = max_occurrence_value
                peak_url_hostname = url_hostname

        # Extract keys and values from the occurrence dictionary
        time_intervals = list(occurrence_dictionary.keys())
        occurrence_values = list(occurrence_dictionary.values())

        # Apply bandpass filtering in terms of time
        lowcut_time = 5  # 5 seconds
        highcut_time = 1000  # 1000 seconds

        # Check if there are enough elements to calculate sampling rate
        if len(time_intervals) >= 2:
            sampling_rate = 1.0 / (time_intervals[1] - time_intervals[0])  # Sampling frequency

            try:
                filtered_occurrence_values = bandpass_filter(occurrence_values, lowcut_time, highcut_time, sampling_rate)
            except ValueError as e:
                print(f"Error: {e}")
                filtered_occurrence_values = occurrence_values

            # Print the filtered occurrence values for debugging
            print("Filtered Occurrence Values:", filtered_occurrence_values)

            # Calculate the average occurrence
            average_occurrence = sum(filtered_occurrence_values) / len(filtered_occurrence_values)

            # Print the average occurrence for debugging
            print("Average Occurrence:", average_occurrence)

            # Subtract average occurrence from all occurrence values
            adjusted_occurrence_values = [occurrence - average_occurrence for occurrence in filtered_occurrence_values]

            # Print the adjusted occurrence values for debugging
            print("Adjusted Occurrence Values:", adjusted_occurrence_values)

            # Remove negative occurrences
            non_negative_occurrence_values = [max(0, occurrence) for occurrence in adjusted_occurrence_values]

            # Get indices for the time range of interest (5 to 1000 seconds)
            time_range_indices = [i for i, t in enumerate(time_intervals) if 5 <= t <= 1000]

            # Print the time range indices for debugging
            print("Time Range Indices:", time_range_indices)

            # Plot the adjusted data within the specified time range
            plt.plot([time_intervals[i] for i in time_range_indices], [non_negative_occurrence_values[i] for i in time_range_indices], label=url_hostname)

    # Print the URL hostname with the peak occurrence
    if peak_url_hostname:
        print(f"\nURL Hostname with Peak Occurrence: {peak_url_hostname}")
        print(f"Peak Occurrence Value: {peak_occurrence_value}")

    # Check if there are multiple URLs in the whitelist before creating legend
    if len(whitelist) > 1:
        plt.legend()

    plt.xlabel("Time Interval (seconds)")  # Change x-axis label
    plt.ylabel("Adjusted Occurrence")  # Change y-axis label
    plt.title("Adjusted Occurrence over Time")  # Change the chart title
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")
