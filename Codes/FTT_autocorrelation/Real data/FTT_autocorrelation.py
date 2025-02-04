from influxdb_client import InfluxDBClient
import numpy as np
import matplotlib.pyplot as plt
from scipy.fft import fft, fftfreq
from scipy.signal import find_peaks

# Load configuration from config.json
config_path = r"C:\Allianz\4\0211\1201\config.json"
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

url = config["url"]
token = config["token"]
org = config["org"]
bucket = config["bucket"]

# Create an InfluxDB client
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

# Query to fetch beaconing data
query = '''
from(bucket: "Net")
  |> range(start: 2023-08-01T00:00:00Z, stop: 2023-08-02T00:00:00Z)
  |> filter(fn: (r) => r["_measurement"] == "hostnames")
  |> filter(fn: (r) => r["url_hostname"] == "saml.allianz.com")
  |> keep(columns: ["_time"])
'''

# Fetch data from InfluxDB
result = query_api.query(query)

# Extract timestamps
timestamps = [record.get_time() for table in result for record in table.records]

# Convert timestamps to seconds since the first timestamp
timestamps = np.array([(t - timestamps[0]).total_seconds() for t in timestamps])

# Create a time series with gaps for missing data
# Define a time grid with a fixed sampling rate (e.g., 1 second)
time_grid = np.arange(0, timestamps[-1] + 1, 1)  # 1-second resolution
values = np.zeros_like(time_grid, dtype=float)

# Fill in the values where data exists
for t in timestamps:
    idx = int(t)  # Convert timestamp to index in the time grid
    values[idx] = 1  # Mark the presence of data

# Apply Fourier Transform to detect periodicity
def apply_fourier_transform(time_grid, values):
    # Compute the Fast Fourier Transform (FFT) using scipy
    n = len(values)
    fft_values = fft(values)
    freqs = fftfreq(n, d=1.0)  # Frequency bins (d=1.0 for 1-second sampling)

    # Plot the frequency spectrum
    plt.figure(figsize=(10, 6))
    plt.plot(freqs[:n // 2], np.abs(fft_values[:n // 2]))
    plt.title("Frequency Spectrum (Fourier Transform)")
    plt.xlabel("Frequency (Hz)")
    plt.ylabel("Amplitude")
    plt.grid()
    plt.show()

    # Find the dominant frequency
    dominant_freq = freqs[np.argmax(np.abs(fft_values[:n // 2]))]
    print(f"Dominant frequency: {dominant_freq} Hz")

# Apply Autocorrelation to detect repeating patterns
def apply_autocorrelation(values):
    print(values)
    # Compute the autocorrelation function
    autocorr = np.correlate(values, values, mode='full')[len(values) - 1:]
    autocorr = autocorr / np.max(autocorr)  # Normalize autocorrelation values
    lags = np.arange(len(autocorr))
    print(autocorr)
    print(lags)
    # Plot the autocorrelation function
    plt.figure(figsize=(10, 6))
    plt.plot(lags, autocorr)
    plt.title("Autocorrelation Function")
    plt.xlabel("Lag")
    plt.ylabel("Autocorrelation")
    plt.grid()
    plt.show()

    # Find the lag with the highest autocorrelation (excluding lag 0)
    peaks, _ = find_peaks(autocorr[1:], height=0.5)  # Adjust height threshold as needed
    if len(peaks) > 0:
        max_lag = peaks[0] + 1  # Add 1 to account for the exclusion of lag 0
        print(f"Lag with highest autocorrelation: {max_lag}")
    else:
        print("No significant autocorrelation peaks found.")

# Apply Fourier Transform
apply_fourier_transform(time_grid, values)

# Apply Autocorrelation
apply_autocorrelation(values)

# Close the InfluxDB client
client.close()