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
								"8180565407	49711957	69	2010-10-07 08:48:35	2010-10-10 03:01:47			FreeReplacement	0.41300	5.90000	1	0.05000	None	0.00000	N	N	N			7385 CLINTON	Apt 24		INDIANAPOLIS	IN	46201	USA	(105)037-5575	Casey Mahon	Casey.Mahon@sitebilgi.net	OS22196-563554-06-11957	http://myretailsite.emc.com/product_detail/Music/?tps=mpumkirvss&sessionid=OS22196-563554-06-11957&ref=oaxmmasx&sku=1643147"));
		mapDriver.withOutput(new IntWritable(49711957), new Text(
				"8180565407,2010-10-07 08:48:35"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("8180565407,2010-10-07 08:48:35"));
		values.add(new Text("6652383320,2010-10-08 08:48:35"));
		values.add(new Text("7149696583,2010-10-09 08:48:35"));
		values.add(new Text("7034197641,2010-10-10 08:48:35"));
		values.add(new Text("6723165345,2010-10-11 08:48:35"));

		reduceDriver.withInput(new IntWritable(49711957), values);
		reduceDriver
				.withOutput(
						NullWritable.get(),
						new Text(
								"49711957	8180565407	2010-10-07 08:48:35	6723165345	2010-10-11 08:48:35"));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		mapReduceDriver
				.withInput(
						new LongWritable(1),
						new Text(
								"8180565407	49711957	69	2010-10-07 08:48:35	2010-10-10 03:01:47			FreeReplacement	0.41300	5.90000	1	0.05000	None	0.00000	N	N	N			7385 CLINTON	Apt 24		INDIANAPOLIS	IN	46201	USA	(105)037-5575	Casey Mahon	Casey.Mahon@sitebilgi.net	OS22196-563554-06-11957	http://myretailsite.emc.com/product_detail/Music/?tps=mpumkirvss&sessionid=OS22196-563554-06-11957&ref=oaxmmasx&sku=1643147"));
		mapReduceDriver
				.withInput(
						new LongWritable(2),
						new Text(
								"6652383320	49711957	48	2010-10-08 08:48:35	2010-10-04 16:46:32			Credit	35.83930	511.99000	1	0.05000	None	0.00000	N	N	N			2504 FOUR CORNERS			LIZTON	IN	46149	USA	(901)402-8251	Carey Real	Carey.Real@guru.ua	OS40109-802651-05-52000	http://myretailsite.emc.com/product_detail/Kitchen/?tps=sqvpvmkksm&sessionid=OS40109-802651-05-52000&ref=mpmsezzz&sku=1442090"));
		mapReduceDriver
				.withInput(
						new LongWritable(3),
						new Text(
								"7149696583	49711957	52	2010-10-09 08:48:35	2010-10-08 05:51:03			CreditCard	149.45425	2173.88000	1	0.00000	BOGO-PDX	0.50000	N	N	N			26 KITTYHAWK			PENGILLY	MN	55775	USA	(676)090-4274	Mark Spivey	Mark.Spivey@tivocommunity.com	OS53372-931651-04-77852	http://myretailsite.emc.com/product_detail/Lawn_&_Patio/?tps=kkmuuviups&sessionid=OS53372-931651-04-77852&ref=mjyxxyse&sku=1543264"));
		mapReduceDriver
				.withInput(
						new LongWritable(4),
						new Text(
								"7034197641	49711957	52	2010-10-10 08:48:35	2010-10-08 00:24:02	2010-10-27 20:35:45		GiftCertificate	3.07440	43.92000	2	0.05000	CUST-Ret-NNQLNPRC	0.50000	N	Y	N			8254 INDIAN PALMS			CONVERSE	IN	46919	USA	(526)874-5192	Corine Goetz	Corine.Goetz@sky.com.br	OS54430-009805-06-73524	http://myretailsite.emc.com/product_detail/Kitchen/?tps=mliiiqmrvr&sessionid=OS54430-009805-06-73524&ref=ppyxpseo&sku=1525627"));
		mapReduceDriver
				.withInput(
						new LongWritable(5),
						new Text(
								"6723165345	49711957	60	2010-10-11 08:48:35	2010-10-06 13:37:43			Credit	4.31940	71.99000	1	0.11000	None	0.00000	N	N	N			20 Judge Street			ALLENDALE	SC	29810	USA	(837)288-4418	Bessie Gagliano	Bessie.Gagliano@richarddawkins.net	OS22134-911920-05-38655	http://myretailsite.emc.com/product_detail/Home_Improvement/?tps=kvqsvrmmrk&sessionid=OS22134-911920-05-38655&ref=szapyjss&sku=1181886"));
		mapReduceDriver
				.withOutput(
						NullWritable.get(),
						new Text(
								"49711957	8180565407	2010-10-07 08:48:35	6723165345	2010-10-11 08:48:35"));

		mapReduceDriver.runTest();
	}
}
