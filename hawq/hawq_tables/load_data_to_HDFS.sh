#!/bin/bash

base_dir="/retail_demo"

# Clean up any previous load
echo "hadoop fs -rm -r -skipTrash $base_dir"
hadoop fs -rm -r -skipTrash $base_dir 

echo "hadoop fs -mkdir $base_dir"
hadoop fs -mkdir $base_dir

for file in *.tsv.gz
do
  dir=`echo $file | perl -ne 's/^(.+?)\..+$/$1/;print;'`
  echo "hadoop fs -mkdir $base_dir/$dir"
  hadoop fs -mkdir $base_dir/$dir
  echo "hadoop fs -put $file $base_dir/$dir/"
  hadoop fs -put $file $base_dir/$dir/
done

