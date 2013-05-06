package com.pivotal.hadoop.city.business;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.pivotal.hadoop.utils.YelpDataInputFormat;

public class CityBusinessDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(CityBusinessDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(CityBusinessMapper.class);
		job.setReducerClass(CityBusinessReducer.class);
		job.setInputFormatClass(YelpDataInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		Path output = new Path(args[1]);
		FileSystem.get(new Configuration()).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CityBusinessDriver(), args);
		System.exit(exitCode);
	}
}
