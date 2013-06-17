package com.pivotal.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CustomerFirstLastOrderDateTest {
	MapReduceDriver<LongWritable, Text, IntWritable, Text, NullWritable, Text> mapReduceDriver;
	MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
	ReduceDriver<IntWritable, Text, NullWritable, Text> reduceDriver;

	@Before
	public void setUp() {
		CustomerFirstLastOrderDateMapper mapper = new CustomerFirstLastOrderDateMapper();
		CustomerFirstLastOrderDateReducer reducer = new CustomerFirstLastOrderDateReducer();
		mapDriver = new MapDriver<LongWritable, Text, IntWritable, Text>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<IntWritable, Text, NullWritable, Text>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, IntWritable, Text, NullWritable, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		mapDriver
				.withInput(
						new LongWritable(1),
						new Text(
								"7149696583	16877852	52	2010-10-04 21:41:54	2010-10-08 05:51:03			CreditCard	149.45425	2173.88000	1	0.00000	BOGO-PDX	0.50000	N	N	N			26 KITTYHAWK			PENGILLY	MN	55770	USA	(676)090-4274	Mark Spivey	Mark.Spivey@tivocommunity.com	OS53372-931651-04-77852	http://myretailsite.emc.com/product_detail/Lawn_&_Patio/?"));
		mapDriver.withOutput(new IntWritable(16877852), new Text(
				"7149696583,2010-10-04 21:41:54"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("7149696583,2010-10-01 21:41:54"));
		values.add(new Text("7149696585,2010-10-02 21:41:54"));
		values.add(new Text("7149696586,2010-10-03 21:41:54"));

		reduceDriver.withInput(new IntWritable(16877852), values);
		reduceDriver
				.withOutput(
						NullWritable.get(),
						new Text(
								"16877852	7149696586	2010-10-03 21:41:54	7149696583	2010-10-01 21:41:54"));

						reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		mapReduceDriver
				.withInput(
						new LongWritable(1),
						new Text(
								"7149696583	16877852	52	2010-10-01 21:41:54	2010-10-08 05:51:03			CreditCard	149.45425	2173.88000	1	0.00000	BOGO-PDX	0.50000	N	N	N			26 KITTYHAWK			PENGILLY	MN	55770	USA	(676)090-4274	Mark Spivey	Mark.Spivey@tivocommunity.com	OS53372-931651-04-77852	http://myretailsite.emc.com/product_detail/Lawn_&_Patio/?"));
		mapReduceDriver
				.withInput(
						new LongWritable(2),
						new Text(
								"7149696585	16877852	52	2010-10-02 21:41:54	2010-10-08 05:51:03			CreditCard	149.45425	2173.88000	1	0.00000	BOGO-PDX	0.50000	N	N	N			26 KITTYHAWK			PENGILLY	MN	55770	USA	(676)090-4274	Mark Spivey	Mark.Spivey@tivocommunity.com	OS53372-931651-04-77852	http://myretailsite.emc.com/product_detail/Lawn_&_Patio/?"));
		mapReduceDriver
				.withInput(
						new LongWritable(3),
						new Text(
								"7149696586	16877852	52	2010-10-03 21:41:54	2010-10-08 05:51:03			CreditCard	149.45425	2173.88000	1	0.00000	BOGO-PDX	0.50000	N	N	N			26 KITTYHAWK			PENGILLY	MN	55770	USA	(676)090-4274	Mark Spivey	Mark.Spivey@tivocommunity.com	OS53372-931651-04-77852	http://myretailsite.emc.com/product_detail/Lawn_&_Patio/?"));
		mapReduceDriver
				.withOutput(
						NullWritable.get(),
						new Text(
								"16877852	7149696586	2010-10-03 21:41:54	7149696583	2010-10-01 21:41:54"));

		mapReduceDriver.runTest();
	}
}
