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

public class CustormerFistLastOrderDateReducer extends
		Reducer<IntWritable, Text, NullWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> counts, Context context)
			throws IOException, InterruptedException {
		StringBuffer temp;
		Text result = new Text();
		StringBuffer lastOrderDate = null;
		StringBuffer firstOrderDate = null;
		String flag;
		ArrayList<String> list = new ArrayList<String>();

		for (Text t : counts) {
			list.add(t.toString());
		}

		try {

			flag = "first";
			firstOrderDate = new StringBuffer(lastOrderDate(key, list, flag));

			flag = "last";
			lastOrderDate = new StringBuffer(lastOrderDate(key, list, flag));

		} catch (ParseException e) {
			e.printStackTrace();
		}
		temp = new StringBuffer(key.toString());
		temp.append("\t");

		temp.append(lastOrderDate);
		temp.append("\t");
		temp.append(firstOrderDate);
		result.set(temp.toString());

		context.write(NullWritable.get(), result);
	}

	public static String lastOrderDate(IntWritable key,
			ArrayList<String> counts, String flag) throws ParseException {

		boolean firstTime = true;
		String olderDate_Id = null;
		Date olderDate = null;
		for (String val : counts) {

			String[] rawTokens = val.toString().split(",");
			String order_date = rawTokens[1];
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

			if (firstTime) {
				olderDate = sf.parse(order_date);
				olderDate_Id = val.toString();
				firstTime = false;
			} else {

				if (flag.equalsIgnoreCase("first")) {
					if (olderDate.before(sf.parse(order_date))) {
						olderDate = sf.parse(order_date);
						olderDate_Id = val.toString();

					} else {
						if (flag.equalsIgnoreCase("last")) {

							if (olderDate.after(sf.parse(order_date))) {
								olderDate = sf.parse(order_date);
								olderDate_Id = val.toString();
							}

						}
					}
				}
			}
		}
		return olderDate_Id.replaceAll(",", "\t");
	}

}
