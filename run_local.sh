#!/bin/bash

echo "=============================================================="
echo " Weather Anomaly Detection - Local Runner (Unix/Linux)"
echo "=============================================================="

JAR="target/weather-anomaly-detector-1.0-SNAPSHOT.jar"

# Check Hadoop
if ! command -v hadoop &> /dev/null; then
    echo "[ERROR] Hadoop is not installed or not in PATH. Please install Hadoop 3.x."
    exit 1
fi

# Check JAR exists
if [ ! -f "$JAR" ]; then
    echo "[ERROR] Pre-built JAR not found at $JAR."
    echo "        Please build it first: brew install maven && mvn clean package"
    exit 1
fi

# Generate sample data if not present
if [ ! -f sample_data.csv ]; then
    echo "[INFO] Generating sample_data.csv using Python..."
    if command -v python3 &> /dev/null; then
        python3 generate_sample_data.py
    elif command -v python &> /dev/null; then
        python generate_sample_data.py
    else
        echo "[WARNING] Python is not installed. Cannot generate sample data automatically."
    fi
fi

# Clean up previous outputs
rm -rf stage1_output stage2_output

echo
echo "[INFO] Running MapReduce pipeline in local mode using Hadoop..."
echo

# Run in local filesystem mode (no HDFS/YARN required)
hadoop jar "$JAR" \
    -Dfs.defaultFS=file:/// \
    -Dmapreduce.framework.name=local \
    sample_data.csv stage1_output stage2_output

if [ $? -ne 0 ]; then
    echo
    echo "[ERROR] MapReduce execution failed!"
    exit 1
fi

echo
echo "=============================================================="
echo " Pipeline Completed Successfully!"
echo "=============================================================="
echo

echo "Stage 1 Output (Baseline Stats - Mean, Sigma):"
echo "-------------------------------------------------------------("
if [ -f stage1_output/part-r-00000 ]; then
    cat stage1_output/part-r-00000
else
    echo "[ERROR] stage1_output/part-r-00000 not found!"
fi

echo
echo "Stage 2 Output (Detected Anomalies - Temp > 3 Sigma):"
echo "-------------------------------------------------------------("
if [ -f stage2_output/part-m-00000 ]; then
    cat stage2_output/part-m-00000
else
    echo "[ERROR] stage2_output/part-m-00000 not found!"
fi
echo
