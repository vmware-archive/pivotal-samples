package com.pivotal.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomerFirstLastOrderDateReducer extends
		Reducer<IntWritable, Text, NullWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> counts, Context context)
			throws IOException, InterruptedException {
		StringBuffer temp;
		Text result = new Text();
		StringBuffer firstlastOrderDate = null;

		try {
			firstlastOrderDate = new StringBuffer(lastOrderDate(key, counts));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		temp = new StringBuffer(key.toString());
		temp.append("\t");
		temp.append(firstlastOrderDate);
		result.set(temp.toString());
		context.write(NullWritable.get(), result);
	}

	

	public static String lastOrderDate(IntWritable key, Iterable<Text> counts)
			throws ParseException {
		boolean firstTime = true;
		String olderDate_Id = null;
		Date olderDate = null;
		String lastDate_Id = null;
		Date lastDate = null;
		for (Text val : counts) {
			String[] rawTokens = val.toString().split(",");
			String order_date = rawTokens[1];
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
			if (firstTime) {
				olderDate = sf.parse(order_date);
				olderDate_Id = val.toString();
				lastDate = sf.parse(order_date);
				lastDate_Id = val.toString();
				firstTime = false;
			} else {
				if (olderDate.before(sf.parse(order_date))) {
					olderDate = sf.parse(order_date);
					olderDate_Id = val.toString();
				} else {
					if (lastDate.after(sf.parse(order_date))) {
						lastDate = sf.parse(order_date);
						lastDate_Id = val.toString();
					}
				}
			}
		}
		String valDate = olderDate_Id.replaceAll(",", "\t") + "\t"
				+ lastDate_Id.replaceAll(",", "\t");
		
		return valDate;
	}

}
