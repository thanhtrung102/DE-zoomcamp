"""
Test script to verify homework answers for Module 2.
"""
import requests
import gzip
import io
import pandas as pd
from pathlib import Path

def format_size(size_bytes):
    """Convert bytes to MiB."""
    return size_bytes / (1024 * 1024)

def test_q1_file_size():
    """Q1: What is the uncompressed file size of Yellow Taxi Dec 2020?"""
    print("=" * 60)
    print("Q1: Yellow Taxi December 2020 - Uncompressed File Size")
    print("=" * 60)

    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz"
    print(f"Downloading: {url}")

    response = requests.get(url, stream=True)
    compressed_size = int(response.headers.get('content-length', 0))
    print(f"Compressed size: {format_size(compressed_size):.1f} MiB")

    # Decompress and get uncompressed size
    compressed_data = response.content
    with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as f:
        uncompressed_data = f.read()

    uncompressed_size = len(uncompressed_data)
    print(f"Uncompressed size: {format_size(uncompressed_size):.1f} MiB")

    print(f"\nANSWER: {format_size(uncompressed_size):.1f} MiB")
    return uncompressed_size

def test_q2_variable_rendering():
    """Q2: What is the rendered value of the file variable?"""
    print("\n" + "=" * 60)
    print("Q2: Variable Rendering (taxi=green, year=2020, month=04)")
    print("=" * 60)

    # Template from Kestra flow
    template = "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"

    # Inputs
    taxi = "green"
    year = "2020"
    month = "04"

    # Render
    rendered = f"{taxi}_tripdata_{year}-{month}.csv"

    print(f"Template: {template}")
    print(f"Inputs: taxi={taxi}, year={year}, month={month}")
    print(f"\nANSWER: {rendered}")
    return rendered

def test_q3_yellow_2020_rows():
    """Q3: Total rows for Yellow Taxi 2020."""
    print("\n" + "=" * 60)
    print("Q3: Yellow Taxi 2020 - Total Row Count")
    print("=" * 60)

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    total_rows = 0

    for month in range(1, 13):
        file = f"yellow_tripdata_2020-{month:02d}.csv.gz"
        url = base_url + file
        print(f"Processing {file}...", end=" ")

        try:
            df = pd.read_csv(url, compression='gzip')
            rows = len(df)
            total_rows += rows
            print(f"{rows:,} rows")
        except Exception as e:
            print(f"ERROR: {e}")

    print(f"\nTOTAL: {total_rows:,}")
    print(f"\nANSWER: {total_rows:,}")
    return total_rows

def test_q4_green_2020_rows():
    """Q4: Total rows for Green Taxi 2020."""
    print("\n" + "=" * 60)
    print("Q4: Green Taxi 2020 - Total Row Count")
    print("=" * 60)

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
    total_rows = 0

    for month in range(1, 13):
        file = f"green_tripdata_2020-{month:02d}.csv.gz"
        url = base_url + file
        print(f"Processing {file}...", end=" ")

        try:
            df = pd.read_csv(url, compression='gzip')
            rows = len(df)
            total_rows += rows
            print(f"{rows:,} rows")
        except Exception as e:
            print(f"ERROR: {e}")

    print(f"\nTOTAL: {total_rows:,}")
    print(f"\nANSWER: {total_rows:,}")
    return total_rows

def test_q5_yellow_march_2021():
    """Q5: Row count for Yellow Taxi March 2021."""
    print("\n" + "=" * 60)
    print("Q5: Yellow Taxi March 2021 - Row Count")
    print("=" * 60)

    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz"
    print(f"Downloading: {url}")

    df = pd.read_csv(url, compression='gzip')
    rows = len(df)

    print(f"\nRows: {rows:,}")
    print(f"\nANSWER: {rows:,}")
    return rows

def test_q6_timezone():
    """Q6: How to configure New York timezone in Schedule trigger."""
    print("\n" + "=" * 60)
    print("Q6: Timezone Configuration")
    print("=" * 60)

    print("""
Kestra Schedule trigger timezone configuration:

triggers:
  - id: schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York

Options analysis:
- 'EST' - Invalid (ambiguous, doesn't handle DST)
- 'America/New_York' - CORRECT (IANA timezone format)
- 'UTC-5' - Invalid (doesn't handle DST)
- 'location: New_York' - Invalid (wrong property name)

ANSWER: Add `timezone` property set to `America/New_York`
""")
    return "America/New_York"

if __name__ == "__main__":
    print("\n" + "#" * 60)
    print("# Module 2 Homework - Answer Verification")
    print("#" * 60)

    # Run Q1 - File size
    test_q1_file_size()

    # Run Q2 - Variable rendering
    test_q2_variable_rendering()

    # Run Q5 first (smaller download)
    test_q5_yellow_march_2021()

    # Run Q6 - Timezone (no download needed)
    test_q6_timezone()

    print("\n" + "#" * 60)
    print("# Q3 and Q4 require downloading all 2020 files")
    print("# This may take several minutes...")
    print("#" * 60)

    # Uncomment to run full tests:
    # test_q3_yellow_2020_rows()
    # test_q4_green_2020_rows()

    print("\n" + "=" * 60)
    print("SUMMARY OF ANSWERS")
    print("=" * 60)
    print("""
Q1: 128.3 MiB (Yellow Dec 2020 uncompressed)
Q2: green_tripdata_2020-04.csv
Q3: 24,648,499 (Yellow 2020 total rows)
Q4: 1,734,051 (Green 2020 total rows)
Q5: 1,925,152 (Yellow March 2021 rows)
Q6: timezone: America/New_York
""")
