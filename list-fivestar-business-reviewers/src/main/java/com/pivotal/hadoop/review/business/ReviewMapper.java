package com.pivotal.hadoop.review.business;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<LongWritable, MapWritable, Text, Text> {

	private static final int REVIEW_RATING = 4;
	Writable businessIdKey = new Text();
	Writable userIdKey = new Text();
	Writable starKey = new Text();

	Text userId = new Text();
	Text outputvalue = new Text();
	Text businessId = new Text();

	public void map(LongWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {

		starKey = value.get(new Text("stars"));
		userIdKey = value.get(new Text("user_id"));
		businessId = (Text)value.get(new Text("business_id"));

		if (StringUtils.isNotEmpty(userIdKey.toString())
				&& StringUtils.isNotEmpty(businessId.toString())
				&& checkReview(Double.parseDouble(starKey.toString()))) {
			userId.set(userIdKey.toString());
			outputvalue.set("R:" + userIdKey.toString() + ":"
					+ starKey.toString());
			context.write(businessId, outputvalue);
		}

	}

	public static boolean checkReview(double reviewCount) {
		return (reviewCount >= REVIEW_RATING) ? true : false;
	}
}
