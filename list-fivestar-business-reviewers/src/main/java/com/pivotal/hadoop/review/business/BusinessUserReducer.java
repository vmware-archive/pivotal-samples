package com.pivotal.hadoop.review.business;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessUserReducer extends Reducer<Text, Text, Text, Text> {

	Text businessName = new Text();
	Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		boolean first_time = true;
		int count = 0;
		StringBuffer userList = new StringBuffer();
		for (Text value : values) {

			if (StringUtils.contains(value.toString(), "B:")) {
				StringBuffer outputKey = new StringBuffer(
						getName(value.toString()));
				outputKey.append(",");
				outputKey.append(getCity(value.toString()));
				outputKey.append(",");
				businessName.set(outputKey.toString());
			}

			if (first_time) {
				userList.append(",");
				if (StringUtils.contains(value.toString(), "R:")) {
					userList.append(getUserid(value.toString()));
				}
				first_time = false;
			} else {
				if (StringUtils.contains(value.toString(), "R:")) {
					userList.append(getUserid(value.toString()) + ",");
				}
			}
			count++;
		}
		userList.delete(userList.length() - 1, userList.length());
		userList.insert(0, count);
		outputValue.set(userList.toString());
		context.write(businessName, outputValue);

	}

	public static String getName(String value) {
		String tokens[] = StringUtils.split(value, ":");
		return tokens[1];
	}

	public static String getCity(String value) {
		String tokens[] = StringUtils.split(value, ":");
		return tokens[2];
	}

	public static String getUserid(String val) {
		String tokens[] = StringUtils.split(val, ":");
		return tokens[1];
	}

}