package com.pivotal.hadoop.utils;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YelpDataRecordReader extends
		RecordReader<LongWritable, MapWritable> {
	private static final Logger LOG = LoggerFactory
			.getLogger(YelpDataRecordReader.class);

	private LineRecordReader reader = new LineRecordReader();

	private final MapWritable value = new MapWritable();
	private final JSONParser jsonParser = new JSONParser();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		reader.initialize(split, context);
	}

	@Override
	public synchronized void close() throws IOException {
		reader.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return reader.getCurrentKey();
	}

	@Override
	public MapWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while (reader.nextKeyValue()) {
			value.clear();
			try {
				try {
					if (parseLineToJSON(jsonParser, reader.getCurrentValue(),
							value)) {
						return true;
					}
				} catch (ParseException e) {
					e.printStackTrace();
					LOG.info("Parse Erorr", e.toString());
				}
			} catch (org.json.simple.parser.ParseException e) {
				e.printStackTrace();
				LOG.info("Parse Erorr", e.toString());
			}
		}
		return false;
	}

	public static boolean parseLineToJSON(JSONParser parser, Text line,
			MapWritable value) throws org.json.simple.parser.ParseException,
			ParseException {
		try {
			JSONObject jsonObj = (JSONObject) parser.parse(line.toString());
			for (Object key : jsonObj.keySet()) {
				Text mapKey = new Text(key.toString());
				Text mapValue = new Text();
				if (jsonObj.get(key) != null) {
					mapValue.set(jsonObj.get(key).toString());
				}

				value.put(mapKey, mapValue);
			}
			return true;
		} catch (NumberFormatException e) {
			LOG.warn("Parsing Error in Number Field" + line, e);
			return false;
		}
	}
}
