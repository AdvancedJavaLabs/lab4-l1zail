#!/bin/bash
# docker build -t wxwmatt/hadoop-standalone:2.1.1-hadoop3.3.1-java8 .
# Параметры входных и выходных директорий
INPUT_DIR="/data/input"
OUTPUT_DIR="/res/output"
JAR_PATH="/jars/lab4-l1zail-1.0.jar"
CLASS_NAME="SalesAnalysator"

echo "Start time: $(date)"

echo "workers,split_size,time" > results.csv

for workers in 2 4 6 8 16; do
    for split_size in 131072 1048576 16777216 67108864; do
        echo "Cleaning up output directory..."
        sudo rm -rf ./res/*
        echo "Running with workers=$workers and split_size=$split_size..."

        START_TIME=$(date +%s)
        echo $START_TIME
        ./hadoop jar $JAR_PATH $CLASS_NAME $INPUT_DIR $OUTPUT_DIR $workers $split_size

        END_TIME=$(date +%s)
        echo $END_TIME

        EXEC_TIME=$((END_TIME - START_TIME))

        echo "$workers,$split_size,$EXEC_TIME" >> results.csv
    done
done

echo "End time: $(date)"

echo "Results saved in results.csv"
