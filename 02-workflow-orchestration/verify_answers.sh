#!/bin/bash
# Verify Homework 2 Answers
# Run this script in your Codespace to verify each answer

echo "=============================================="
echo "Module 2 Homework - Answer Verification"
echo "=============================================="

# Q1: File Size
echo ""
echo "Q1: Yellow Taxi Dec 2020 - Uncompressed File Size"
echo "-------------------------------------------"
echo "Downloading and checking file size..."
wget -q https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz -O /tmp/yellow_2020_12.csv.gz
gunzip -f /tmp/yellow_2020_12.csv.gz
SIZE=$(ls -lh /tmp/yellow_2020_12.csv | awk '{print $5}')
echo "Uncompressed size: $SIZE"
echo "Expected: 128.3 MiB or ~128M"
rm -f /tmp/yellow_2020_12.csv

# Q2: Variable Rendering
echo ""
echo "Q2: Variable Rendering"
echo "-------------------------------------------"
echo "Template: {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
echo "Inputs: taxi=green, year=2020, month=04"
echo "Rendered: green_tripdata_2020-04.csv"
echo "Expected: green_tripdata_2020-04.csv"

# Q5: Yellow March 2021 (testing this first - smaller)
echo ""
echo "Q5: Yellow Taxi March 2021 - Row Count"
echo "-------------------------------------------"
echo "Counting rows in yellow_tripdata_2021-03.csv.gz..."
ROWS_Q5=$(wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz | gunzip | wc -l)
ROWS_Q5=$((ROWS_Q5 - 1))  # Subtract header
echo "Row count: $ROWS_Q5"
echo "Expected: 1,925,152"

# Q6: Timezone
echo ""
echo "Q6: Timezone Configuration"
echo "-------------------------------------------"
echo "Correct Kestra configuration:"
echo "  timezone: America/New_York"
echo "Expected: America/New_York (IANA format)"

echo ""
echo "=============================================="
echo "Q3 and Q4 require downloading ALL 2020 files"
echo "Run the Python script for full verification:"
echo "  python verify_answers.py"
echo "=============================================="

echo ""
echo "QUICK SUMMARY"
echo "=============================================="
echo "Q1: 128.3 MiB"
echo "Q2: green_tripdata_2020-04.csv"
echo "Q3: 24,648,499 (run Python script to verify)"
echo "Q4: 1,734,051 (run Python script to verify)"
echo "Q5: 1,925,152"
echo "Q6: timezone: America/New_York"
