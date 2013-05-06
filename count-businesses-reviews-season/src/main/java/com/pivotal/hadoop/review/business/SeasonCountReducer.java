package com.pivotal.hadoop.review.business;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SeasonCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable seasonCount = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable x : values) {
			count += 1;
		}
		seasonCount.set(count);
		context.write(key, seasonCount);
	}

}