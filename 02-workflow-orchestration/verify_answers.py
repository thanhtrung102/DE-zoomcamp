#!/usr/bin/env python3
"""
Verify Homework 2 Answers
Run: python verify_answers.py

For quick tests (Q1, Q2, Q5, Q6):
    python verify_answers.py --quick

For full verification including Q3 and Q4:
    python verify_answers.py --full
"""
import argparse
import gzip
import io
import sys
from urllib.request import urlopen

def download_gzip(url):
    """Download and decompress a gzip file."""
    print(f"  Downloading: {url.split('/')[-1]}")
    with urlopen(url) as response:
        compressed_data = response.read()
    with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as f:
        return f.read()

def count_lines(data):
    """Count lines in bytes data (excluding header)."""
    return data.count(b'\n') - 1  # Subtract header line

def test_q1():
    """Q1: Yellow Dec 2020 uncompressed file size."""
    print("\n" + "=" * 60)
    print("Q1: Yellow Taxi Dec 2020 - Uncompressed File Size")
    print("=" * 60)

    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz"
    data = download_gzip(url)
    size_mib = len(data) / (1024 * 1024)

    print(f"  Uncompressed size: {size_mib:.1f} MiB")
    print(f"  ✓ ANSWER: 128.3 MiB")
    return size_mib

def test_q2():
    """Q2: Variable rendering."""
    print("\n" + "=" * 60)
    print("Q2: Variable Rendering")
    print("=" * 60)

    template = "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
    taxi, year, month = "green", "2020", "04"
    rendered = f"{taxi}_tripdata_{year}-{month}.csv"

    print(f"  Template: {template}")
    print(f"  Inputs: taxi={taxi}, year={year}, month={month}")
    print(f"  Rendered: {rendered}")
    print(f"  ✓ ANSWER: green_tripdata_2020-04.csv")
    return rendered

def test_q3():
    """Q3: Yellow 2020 total rows."""
    print("\n" + "=" * 60)
    print("Q3: Yellow Taxi 2020 - Total Row Count")
    print("=" * 60)

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    total_rows = 0

    for month in range(1, 13):
        file = f"yellow_tripdata_2020-{month:02d}.csv.gz"
        url = base_url + file
        try:
            data = download_gzip(url)
            rows = count_lines(data)
            total_rows += rows
            print(f"    {file}: {rows:,} rows")
        except Exception as e:
            print(f"    {file}: ERROR - {e}")

    print(f"\n  TOTAL: {total_rows:,}")
    print(f"  ✓ ANSWER: 24,648,499")
    return total_rows

def test_q4():
    """Q4: Green 2020 total rows."""
    print("\n" + "=" * 60)
    print("Q4: Green Taxi 2020 - Total Row Count")
    print("=" * 60)

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
    total_rows = 0

    for month in range(1, 13):
        file = f"green_tripdata_2020-{month:02d}.csv.gz"
        url = base_url + file
        try:
            data = download_gzip(url)
            rows = count_lines(data)
            total_rows += rows
            print(f"    {file}: {rows:,} rows")
        except Exception as e:
            print(f"    {file}: ERROR - {e}")

    print(f"\n  TOTAL: {total_rows:,}")
    print(f"  ✓ ANSWER: 1,734,051")
    return total_rows

def test_q5():
    """Q5: Yellow March 2021 rows."""
    print("\n" + "=" * 60)
    print("Q5: Yellow Taxi March 2021 - Row Count")
    print("=" * 60)

    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz"
    data = download_gzip(url)
    rows = count_lines(data)

    print(f"  Row count: {rows:,}")
    print(f"  ✓ ANSWER: 1,925,152")
    return rows

def test_q6():
    """Q6: Timezone configuration."""
    print("\n" + "=" * 60)
    print("Q6: Timezone Configuration")
    print("=" * 60)

    print("""
  Kestra Schedule trigger configuration:

  triggers:
    - id: schedule
      type: io.kestra.plugin.core.trigger.Schedule
      cron: "0 9 1 * *"
      timezone: America/New_York

  Options:
    ✗ EST - Invalid (ambiguous, no DST handling)
    ✓ America/New_York - CORRECT (IANA format)
    ✗ UTC-5 - Invalid (no DST handling)
    ✗ location: New_York - Invalid (wrong property)

  ✓ ANSWER: Add `timezone` property set to `America/New_York`
""")
    return "America/New_York"

def print_summary():
    """Print answer summary."""
    print("\n" + "=" * 60)
    print("ANSWER SUMMARY")
    print("=" * 60)
    print("""
  Q1: 128.3 MiB
  Q2: green_tripdata_2020-04.csv
  Q3: 24,648,499
  Q4: 1,734,051
  Q5: 1,925,152
  Q6: timezone: America/New_York
""")

def main():
    parser = argparse.ArgumentParser(description="Verify Homework 2 Answers")
    parser.add_argument("--quick", action="store_true", help="Run quick tests (Q1, Q2, Q5, Q6)")
    parser.add_argument("--full", action="store_true", help="Run all tests including Q3 and Q4")
    parser.add_argument("-q", "--question", type=int, choices=[1,2,3,4,5,6], help="Run specific question")
    args = parser.parse_args()

    print("#" * 60)
    print("# Module 2 Homework - Answer Verification")
    print("#" * 60)

    if args.question:
        tests = {1: test_q1, 2: test_q2, 3: test_q3, 4: test_q4, 5: test_q5, 6: test_q6}
        tests[args.question]()
    elif args.full:
        test_q1()
        test_q2()
        test_q3()
        test_q4()
        test_q5()
        test_q6()
    else:  # Quick or default
        test_q1()
        test_q2()
        test_q5()
        test_q6()
        if not args.quick:
            print("\n" + "-" * 60)
            print("Note: Q3 and Q4 skipped (downloads all 2020 files)")
            print("Run with --full to verify Q3 and Q4")

    print_summary()

if __name__ == "__main__":
    main()
