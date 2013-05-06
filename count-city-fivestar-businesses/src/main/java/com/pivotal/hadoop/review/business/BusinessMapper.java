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

	private static final double STAR_RATING = 5.0;
	
	Writable stars = new Text("stars");
	Writable businessKey = new Text("name");
	Writable cityKey = new Text("city");
	Text star = new Text();
	Text businessName = new Text();
	Text city = new Text();

	@Override
	protected void map(LongWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {

		star = (Text) value.get(stars);
		businessName = (Text) value.get(businessKey);
		city = (Text) value.get(cityKey);

		if (StringUtils.isNotEmpty(star.toString())
				&& StringUtils.isNotEmpty(businessName.toString())) {
			if (testForStarRating(Double.parseDouble(star.toString()))) {
				context.write(city, businessName);
			}
		}
	}

	public static boolean testForStarRating(double reviewCount) {
		return (reviewCount >= STAR_RATING) ? true : false;
	}

}
