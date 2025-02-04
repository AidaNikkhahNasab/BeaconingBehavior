import os
import json
import csv
from concurrent.futures import ProcessPoolExecutor
import matplotlib.pyplot as plt
from collections import Counter

def process_file(file_path):
    """
    Process a single file to determine the number of unique URLs contacted.
    Returns the filename (IP) and the count of unique URLs.
    """
    unique_urls = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    log_entry = json.loads(line)
                    url = log_entry.get("url_hostname")
                    if url:
                        unique_urls.add(url)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

    return (os.path.basename(file_path), len(unique_urls))

def find_files_with_url_counts(folder_path):
    """
    Process all files in the specified folder using multiprocessing
    to identify the number of unique URLs contacted.
    """
    file_paths = [
        os.path.join(folder_path, file_name)
        for file_name in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, file_name))
    ]

    # Use ProcessPoolExecutor for parallel processing
    results = []
    with ProcessPoolExecutor() as executor:
        for result in executor.map(process_file, file_paths):
            if result:
                results.append(result)

    return results

def save_csv(results, output_csv_path):
    """
    Save the processed results to a CSV file.
    """
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["IP Address", "Unique URL Count"])
        csv_writer.writerows(results)

    print(f"Results saved to {output_csv_path}")

def generate_chart(results, output_chart_path):
    """
    Generate a bar chart showing the distribution of hosts by the number of unique URLs contacted.
    """
    # Extract URL counts and calculate distribution
    url_counts = [count for _, count in results]
    count_distribution = Counter(url_counts)

    # Focus on 1 to 10 URLs for clarity
    x_values = list(range(1, 11))
    y_values = [count_distribution.get(i, 0) for i in x_values]

    # Create the bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(x_values, y_values, color='skyblue', edgecolor='black')
    plt.xlabel('Number of Unique URLs Contacted')
    plt.ylabel('Number of Hosts')
    plt.title('Distribution of Hosts by Number of Unique URLs Contacted')
    plt.xticks(x_values)
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Save the chart to a file and display it
    plt.savefig(output_chart_path)
    print(f"Chart saved to {output_chart_path}")
    plt.show()

if __name__ == "__main__":
    # Specify the folder path containing the files
    folder_path = r"C:\Users\aydan\Downloads\export\EXPORT"

    # Specify the output CSV file path
    output_csv_path = r"C:\Users\aydan\Downloads\export\result1.csv"

    # Specify the output chart file path
    output_chart_path = r"C:\Users\aydan\Downloads\export\result_chart.png"

    # Process files to get URL counts
    results = find_files_with_url_counts(folder_path)

    # Save results to a CSV file
    save_csv(results, output_csv_path)

    # Generate a chart for the results
    generate_chart(results, output_chart_path)
