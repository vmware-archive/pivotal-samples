package com.pivotal.hadoop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PostalCodesReducer extends
		Reducer<IntWritable, Text, NullWritable, Text> {
	TreeMap<Double, Text> recordRepo;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer<IntWritable,Text,NullWritable,Text>.Context context) throws IOException ,InterruptedException {
		recordRepo = new TreeMap<Double, Text>();
	};

	@Override
	protected void reduce(IntWritable key, Iterable<Text> counts,
			Context context) throws IOException, InterruptedException {
		StringBuffer temp;
		double total_tax_amount = 0;
		double total_paid_amount = 0;
		Text result = new Text();
		for (Text val : counts) {
			String[] rawTokens = val.toString().split(",");
			StringBuffer tax_amount = new StringBuffer(rawTokens[0]);
			StringBuffer paid_amount = new StringBuffer(rawTokens[1]);

			total_paid_amount = total_paid_amount
					+ Double.parseDouble(paid_amount.toString());

			total_tax_amount = total_tax_amount
					+ Double.parseDouble(tax_amount.toString());
		}

		temp = new StringBuffer(key.toString());
		temp.append("\t");
		temp.append(new StringBuffer(String.valueOf(total_paid_amount)));
		temp.append("\t");
		temp.append(new StringBuffer(String.valueOf(total_tax_amount)));

		result.set(temp.toString());

		recordRepo.put(total_paid_amount, result);
		if (recordRepo.size() > 10) {
			recordRepo.remove(recordRepo.firstKey());
		}
	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		for (Text t : recordRepo.descendingMap().values()) {
			context.write(NullWritable.get(), t);
		}
		super.cleanup(context);
	}
}
