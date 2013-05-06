package com.pivotal.hadoop.review.business;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FiveStarReviewedBusinessReducer extends
		Reducer<Text, Text, Text, IntWritable> {

	IntWritable businessCount = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (Text val : values) {
			count += 1;
		}
		businessCount.set(count);
		context.write(key, businessCount);
	}

}