package com.pivotal.hadoop.utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YelpDataInputFormat extends
		FileInputFormat<LongWritable, MapWritable> {

	private static final Logger log = LoggerFactory
			.getLogger(YelpDataInputFormat.class);

	@Override
	public RecordReader<LongWritable, MapWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new YelpDataRecordReader();
	}
}
