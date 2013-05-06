package com.pivotal.hadoop.city.business;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CityBusinessTest {

	private MapDriver<LongWritable, MapWritable, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	private MapReduceDriver<LongWritable, MapWritable, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() throws Exception {

		CityBusinessMapper mapper = new CityBusinessMapper();
		CityBusinessReducer reducer = new CityBusinessReducer();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testCityBusinessMapper() throws Exception {

		final LongWritable inputKey = new LongWritable(0);

		final Text outputKey = new Text("Surprise");
		final IntWritable outputValue = new IntWritable(1);

		final MapWritable inputValue = new MapWritable();
		inputValue.put(new Text("business_id"), new Text(
				"fecYnd2_OTDECk7bd6GOFw"));
		inputValue.put(new Text("full_address"), new Text(
				"12851 W Bell Rd\nSte 20\nSurprise, AZ 85374"));
		inputValue.put(new Text("open"), new Text("true"));
		inputValue.put(new Text("categories"), new Text("Pizza, Restaurants"));
		inputValue.put(new Text("city"), new Text("Surprise"));
		inputValue.put(new Text("review_count"), new Text("7"));
		inputValue.put(new Text("name"), new Text("Peter Piper Pizza"));
		inputValue.put(new Text("neighborhoods"), new Text("[]"));
		inputValue.put(new Text("longitude"), new Text("-112.3373424"));
		inputValue.put(new Text("state"), new Text("AZ"));
		inputValue.put(new Text("stars"), new Text("4.5"));
		inputValue.put(new Text("latitude"), new Text("33.638134100000002"));
		inputValue.put(new Text("type"), new Text("business"));

		mapDriver.withInput(inputKey, inputValue)
				.withOutput(outputKey, outputValue).runTest();

	}

	@Test
	public void testCityBusinessReducer() throws Exception {

		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
				.withReducer(new CityBusinessReducer())
				.withInputKey(new Text("Surprise"))
				.withInputValues(
						Arrays.asList(new IntWritable(1), new IntWritable(1),
								new IntWritable(1)))
				.withOutput(new Text("Surprise"), new IntWritable(3)).runTest();
	}

	@Test
	public void testCityBusinessMapperReducer() throws Exception {
		final MapWritable inputValue = new MapWritable();
		inputValue.put(new Text("business_id"), new Text(
				"fecYnd2_OTDECk7bd6GOFw"));
		inputValue.put(new Text("full_address"), new Text(
				"12851 W Bell Rd\nSte 20\nSurprise, AZ 85374"));
		inputValue.put(new Text("open"), new Text("true"));
		inputValue.put(new Text("categories"), new Text("Pizza, Restaurants"));
		inputValue.put(new Text("city"), new Text("Surprise"));
		inputValue.put(new Text("review_count"), new Text("7"));
		inputValue.put(new Text("name"), new Text("Peter Piper Pizza"));
		inputValue.put(new Text("neighborhoods"), new Text("[]"));
		inputValue.put(new Text("longitude"), new Text("-112.3373424"));
		inputValue.put(new Text("state"), new Text("AZ"));
		inputValue.put(new Text("stars"), new Text("4.5"));
		inputValue.put(new Text("latitude"), new Text("33.638134100000002"));
		inputValue.put(new Text("type"), new Text("business"));
		mapReduceDriver.withInput(new LongWritable(0), inputValue)

		.withOutput(new Text("Surprise"), new IntWritable(1)).runTest();

	}
}