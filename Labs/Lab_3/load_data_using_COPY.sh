#!/bin/bash

# Load the three small .tsv.gz files using COPY, on HAWQ master
# The files are TAB delimited, where NULL is just the empty string, ''

# $Id: load_data_using_COPY.sh,v 1.1 2013/05/04 10:50:14 gpadmin Exp gpadmin $

schema=retail_demo

for table in `ls *.gz  | perl -ne 's/^(\w+).+$/$1/;print;'`
do
  file="$table.tsv.gz"
  zcat $file | psql -c "COPY $schema.$table FROM STDIN DELIMITER E'\t' NULL E'';"
done


