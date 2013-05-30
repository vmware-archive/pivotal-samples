package com.pivotal.hadoop.jsontotext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonToTextUtill {

	static String[] user = { "votes", "user_id", "name", "average_stars",
		"review_count","type"};
	static String[] business = { "business_id", "full_address", "open",
		"categories", "city", "review_count", "name", "neighborhoods",
		"longitude", "state", "stars", "latitude" ,"type"};

	static String[] review = { "votes", "user_id", "review_id", "stars",
			"date","text", "type", "business_id" };

	public static void main(String[] args) throws IOException, ParseException {

		if (args.length != 3) {
			System.out
					.println("Please enter <tablename> <File location> <outputfile>");
			System.exit(2);
		} else if (!(isTableName(args[0]))) {
			System.out.println("Please enter table name correctly");
		} else if (!(isFileExists(args[1]))) {

			System.out.println("Please enter input file location correctly");
		} else if (!(isJsonFile(args[1]))) {
			System.out.println("Please enter json file ");
		} else {

			String table = args[0];
			String filePath = args[1];
			String output = args[2];
			if (table.contentEquals("user")) {
				parseJson(user, filePath, output);
			} else if (table.contentEquals("business")) {
				parseJson(business, filePath, output);
			} else if (table.contentEquals("review")) {
				parseJson(review, filePath, output);
			}
		}
	}

	private static void parseJson(String[] field, String filePath, String output)
			throws IOException {
		JSONParser jsonParser = new JSONParser();
		File inputFile = new File(filePath);
		Scanner sc = null;
		FileWriter fw = null;
		BufferedWriter bw = null;
		File fileOut = new File(output);

		try {

			sc = new Scanner(inputFile, "utf-8");
			String line;
			fw = new FileWriter(fileOut.getAbsoluteFile());
			bw = new BufferedWriter(fw);

			sc.useDelimiter("}" + System.getProperty("line.separator"));
			while (sc.hasNextLine()) {
				line = sc.nextLine();
				if (line != null || !(line.isEmpty())) {
					JSONObject jsonObj;
					jsonObj = (JSONObject) jsonParser.parse(line);
					String formatRow = formatRow(field, jsonObj);

					bw.write(formatRow);

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bw != null && sc != null && fw != null) {
				bw.close();
				sc.close();
				fw.close();
			}
		}
	}

	public static String formatRow(String[] field, JSONObject jsonObj) {
		StringBuffer row = new StringBuffer();
		int count = 0;
		for (String column : field) {
			count++;
			String temp = jsonObj.get(column).toString();

			if (column.contentEquals("text")
					|| column.contentEquals("full_address")) {

				String col = temp.replace("\n", "\\n");

				temp = col;
			}

			if (column.contentEquals("name") || column.contentEquals("text")) {

				String emt = ":" + "\\";
				if (temp.contains(emt)) {
					String cols = "";

					cols = temp.replace(emt, ".");

					temp = cols;
				}
				if (temp.contains("|")) {
					String cols = "";
					cols = temp.replace("|", ".");
					temp = cols;
				}

			}

			row.append(temp);
			if (count < field.length) {
				row.append("|");
			}

		}
		row.append(System.getProperty("line.separator"));
		return row.toString();
	}

	public static boolean isJsonFile(String file) throws IOException {
		File f = new File(file);
		String fileType = f.getCanonicalPath();
		return fileType.endsWith(".json");
	}

	public static boolean isTableName(String tableName) {
		return (tableName.equalsIgnoreCase("business")
				|| tableName.equalsIgnoreCase("user") || tableName
					.equalsIgnoreCase("review"));
	}

	public static boolean isFileExists(String fileName) {
		File f = new File(fileName);
		return f.exists();
	}

}
