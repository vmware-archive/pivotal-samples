package com.pivotal.hadoop.city.business;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CityBusinessReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int totalRecords = 0;
		for (IntWritable value : values) {
			totalRecords++;
		}
		context.write(key, new IntWritable(totalRecords));
	}

}