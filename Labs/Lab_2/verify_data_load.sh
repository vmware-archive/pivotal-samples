#!/bin/bash

# Verify one of the GPXF/HDFS tables
PAGER=cat psql -c "SELECT * FROM retail_demo.orders LIMIT 5"

