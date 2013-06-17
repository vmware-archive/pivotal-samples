package com.pivotal.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerFirstLastOrderDateMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable key = new IntWritable();
	Text val = new Text();

	@Override
	protected void map(LongWritable offset, Text text, Context context)
			throws IOException, InterruptedException {
		String[] tokens = text.toString().split("\t");
		String order_id = tokens[0];
		String customer_id = tokens[1];
		String order_datetime = tokens[3];

		key.set(Integer.parseInt(customer_id));
		val.set(order_id + "," + order_datetime);
		context.write(key, val);

	}
}