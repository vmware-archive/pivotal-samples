package com.pivotal.hadoop.review.business;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SeasonReviewMapper extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {

	Writable dateKey = new Text("date");
	Text reviewDate = new Text();
	IntWritable one = new IntWritable(1);

	Text monthly = new Text();
	Text summerSeason = new Text("Summer");
	Text winterSeason = new Text("Winter");
	Text springSeason = new Text("Spring");
	Text fallSeason = new Text("Fall");

	// private MultipleOutputs<Text, NullWritable> multileOutputs = null;

	// @SuppressWarnings({ "unchecked", "rawtypes" })
	// @Override
	// protected void setup(Context context) {
	// // Create a new MultipleOutputs using the context object
	// multileOutputs = new MultipleOutputs(context);
	// }

	@Override
	protected void map(LongWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {

		reviewDate = (Text) value.get(dateKey);
		if (reviewDate == null) {
			return;
		}

		if (StringUtils.isNotEmpty(reviewDate.toString())) {

			// date is in this format 2011-07-27
			String tokens[] = StringUtils.split(reviewDate.toString(), "");
			String month = tokens[1];
			writeToSeason(month, context);
			writeToMonth(month, context);
		}
	}

	public void writeToMonth(String month, Context context) {

		String[] months = new String[] { "dummy", "Jan", "Feb", "Mar", "Apr",
				"May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec" };
		int monthNumber;
		monthNumber = Integer.parseInt(month);
		try {
			monthly.set(months[monthNumber]);
			context.write(monthly, one);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void writeToSeason(String month, Context context) {

		int monthNumber;
		monthNumber = Integer.parseInt(month);

		try {
			if (monthNumber >= 11  || monthNumber == 1)
				context.write(winterSeason, one);
			else if (monthNumber >= 2 && monthNumber <= 4)
				context.write(fallSeason, one);
			else if (monthNumber >= 5 && monthNumber <= 7)
				context.write(springSeason, one);
			else if (monthNumber >= 8 && monthNumber <= 10)
				context.write(summerSeason, one);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
