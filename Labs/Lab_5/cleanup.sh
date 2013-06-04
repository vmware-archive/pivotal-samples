#!/bin/bash

rm -rf ./data ./data2
psql -c "DROP TABLE IF EXISTS dataload_lineitems; DROP EXTERNAL TABLE IF EXISTS xf_dataload_lineitems;"
hadoop fs -rm -r -skipTrash /dataload_exercise /dataload_exercise_out

