package com.pivotal.hadoop.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load the file content into a HashMap Content should be of the form
 * name==value
 * 
 * Returns Map of keys and values
 * 
 */
public class FileUtils {

	private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

	public static void main(String[] args) throws UnsupportedEncodingException {
		if (args.length != 1) {
			System.err.println("Usage: " + "FileUtils" + "<fileName>");
			System.exit(2);
		}
		HashMap<String, String> userCache = new HashMap<String, String>();

		userCache = readFile(args[0], new Configuration());
		
		//Test with User cache
		System.out.println(userCache.get(new String("rLtl8ZkDX5vH5nAx9C3q5Q")));
		System.out.println(userCache.get("0a2KyEL0d3Yb1V6aivbIuQ"));
		System.out.println(userCache.get("ltr-xWeLJYTOdc-xZNnQiw"));
		
		//String  x = new String(userCache.get("0a2KyEL0d3Yb1V6aivbIuQ").toString());

        for (Map.Entry entry : userCache.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " +
                               entry.getValue());
            String x = entry.getKey().toString();
            System.out.println("len=" + x.length());
            
            String y=new String("rLtl8ZkDX5vH5nAx9C3q5Q");
            System.out.println("len=" + y.length());
        }
		
	}

	public static HashMap<String, String> readFile(String fileName,
			Configuration conf) {
		Path inputFile = new Path(fileName);

		String line = null;
		FileSystem fs;

		HashMap<String, String> userCache = new HashMap<String, String>();
		try {
			fs = FileSystem.get(conf);
			for (FileStatus status : fs.listStatus(inputFile)) {
				BufferedReader rdr = new BufferedReader(new InputStreamReader(
						fs.open(status.getPath())));
				System.out.println("Reading " + status.getPath());
		
				while ((line = rdr.readLine()) != null) {
					String str = new String(line);
					System.out.println("line = " + str);
					String tokens[] = str.split("==");
					userCache.put(tokens[0], tokens[1]);
				}
				rdr.close();
			}
		} catch (IOException e) {
			LOG.warn("Error in reading file" + inputFile.toString(), e);
			e.printStackTrace();
		}
		return userCache;
	}
}
