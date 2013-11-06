#!/bin/bash

base_dir="/retail_demo"

# Clean up any previous load
echo "hadoop fs -rm -r -skipTrash $base_dir"
hadoop fs -rm -r -skipTrash $base_dir

# Copy the data directory, recursively, into HDFS root
echo "hadoop fs -put /retail_demo /"
hadoop fs -put /retail_demo /

