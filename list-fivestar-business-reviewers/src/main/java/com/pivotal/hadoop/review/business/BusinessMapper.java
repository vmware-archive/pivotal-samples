package com.pivotal.hadoop.review.business;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessMapper extends
		Mapper<LongWritable, MapWritable, Text, Text> {

	Writable businessIdKey = new Text("business_id");
	Writable businessNameKey = new Text("name");
	Writable cityKey = new Text("city");

	Text busninessName = new Text();
	Text outputValue = new Text();
	Text businessId = new Text();
	Text city = new Text();

	public void map(LongWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {

		businessId = (Text) value.get(businessIdKey);
		busninessName = (Text) value.get(businessNameKey);

		city = (Text) value.get(cityKey);

		if (StringUtils.isNotEmpty(businessId.toString())
				&& StringUtils.isNotEmpty(busninessName.toString())) {
			outputValue.set("B:" + busninessName.toString() + ":"
					+ city.toString());
			context.write(businessId, outputValue);
		}

	}
}
