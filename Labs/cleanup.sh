#!/bin/bash

# Clean up HBase: manually through hbase shell
#
# hbase(main):002:0> disable_all '^.+?_dim$'
# hbase(main):003:0> drop_all '^.+?_dim$'
#

# Clean up HAWQ
psql -c "DROP SCHEMA retail_demo CASCADE;"

# Clean up HDFS
hadoop fs -rm -r -skipTrash /retail_demo

