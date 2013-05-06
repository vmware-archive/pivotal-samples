package com.pivotal.hadoop.city.business;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class CityBusinessMapper extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {

	IntWritable one = new IntWritable(1);
	Writable cityKey = new Text("city");
	Writable businessKey = new Text("business_id");

	Text city = new Text();
	Text businessId = new Text();

	@Override
	protected void map(LongWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {

		city = (Text) value.get(cityKey);
		businessId = (Text) value.get(businessKey);

		if (StringUtils.isNotEmpty(city.toString())
				&& StringUtils.isNotEmpty(businessId.toString())) {
			context.write(city, one);
		}
	}

}
