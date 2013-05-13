package com.pivotal.hadoop.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to parse the user.json
 * 
 */
public class UserJSONParserUtil {

	private static final Logger LOG = LoggerFactory
			.getLogger(UserJSONParserUtil.class);

	private final static JSONParser jsonParser = new JSONParser();

	public static void main(String[] args)
			throws org.json.simple.parser.ParseException, ParseException {

		if (args.length != 2) {
			System.out
					.println("UserJSONParserUtil <input_file>  <output_file>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		createUserListFile(args[0], args[1], conf);
	}

	public static void createUserListFile(String inputFile, String outputFile,
			Configuration conf) throws org.json.simple.parser.ParseException,
			ParseException {

		Path inputFile1 = new Path(inputFile);
		Path bfFile = new Path(outputFile);

		String line = null;
		FileSystem fs;

		try {
			fs = FileSystem.get(conf);
			FSDataOutputStream strm = fs.create(bfFile);

			for (FileStatus status : fs.listStatus(inputFile1)) {
				BufferedReader rdr = new BufferedReader(new InputStreamReader(
						fs.open(status.getPath())));
				System.out.println("Reading " + status.getPath());
				while ((line = rdr.readLine()) != null) {
					HashMap<String, String> value = parseLineToJSON(jsonParser,
							line);
					// loadJsonToMap(line);
					System.out.println(value.get("user_id"));
					System.out.println(value.get("name"));
					StringBuffer output = new StringBuffer();
					output.append(value.get("user_id"));
					output.append("==");
					output.append(value.get("name") + "\n");
					strm.writeBytes(output.toString());
				}
				rdr.close();
				strm.flush();
				strm.close();
			}
		} catch (IOException e) {
			LOG.warn("Error in reading file" + inputFile.toString(), e);
			e.printStackTrace();
		}
	}

	public static HashMap<String, String> parseLineToJSON(JSONParser parser,
			String line) throws org.json.simple.parser.ParseException,
			ParseException {
		HashMap<String, String> value = new HashMap<String, String>();
		try {
			JSONObject jsonObj = (JSONObject) parser.parse(line.toString());

			for (Object key : jsonObj.keySet()) {
				String mapKey = new String(key.toString());
				String mapValue = null;
				if (jsonObj.get(key) != null) {
					mapValue = new String(jsonObj.get(key).toString());
				}
				value.put(mapKey, mapValue);
			}
			return value;
		} catch (NumberFormatException e) {
			LOG.warn("Parsing Error in Number Field" + line, e);
			return value;
		}
	}
}
