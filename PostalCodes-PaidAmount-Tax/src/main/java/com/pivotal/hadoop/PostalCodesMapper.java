package com.pivotal.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PostalCodesMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	IntWritable key = new IntWritable();
	Text val = new Text();

	@Override
	protected void map(LongWritable offset, Text text, Context context)
			throws IOException, InterruptedException {
		String[] tokens = text.toString().split("\t");
		String total_tax_amount = tokens[8];
		String total_paid_amount = tokens[9];
		String billing_address_postal_code = tokens[24];
		key.set(Integer.parseInt(billing_address_postal_code));
		val.set(total_tax_amount + "," +total_paid_amount);
		context.write(key, val);

	}
}