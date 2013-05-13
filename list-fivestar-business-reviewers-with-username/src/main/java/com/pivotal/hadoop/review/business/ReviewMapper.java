package com.pivotal.hadoop.review.business;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pivotal.hadoop.utils.FileUtils;

public class ReviewMapper extends Mapper<LongWritable, MapWritable, Text, Text> {

	private static final Logger LOG = LoggerFactory
			.getLogger(ReviewMapper.class);

	private static final int REVIEW_RATING = 4;
	Writable businessIdKey = new Text();
	Writable userIdKey = new Text();
	Writable starKey = new Text();

	Text userId = new Text();
	Text outputvalue = new Text();
	Text businessId = new Text();

	HashMap<String, String> userCache = new HashMap<String, String>();

	protected void setup(Context context) throws IOException,
			InterruptedException {

		URI[] files = DistributedCache
				.getCacheFiles(context.getConfiguration());

		userCache = FileUtils.readFile(files[0].getPath(),
				context.getConfiguration());

		// userCache =
		// FileUtils.readFile("cache/usernames.txt",context.getConfiguration());

	}

	public void map(LongWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {

		starKey = value.get(new Text("stars"));
		userIdKey = value.get(new Text("user_id"));
		businessId = (Text) value.get(new Text("business_id"));

		if (StringUtils.isNotEmpty(userIdKey.toString())
				&& StringUtils.isNotEmpty(businessId.toString())
				&& checkReview(Double.parseDouble(starKey.toString()))) {

			System.out.println("Reading Bloom filter from: "
					+ userCache.get(userIdKey.toString()));
			String userKey = userIdKey.toString();
			String username = userCache.get(userKey);

			if (username != null) {
				System.out.println("Reading Bloom filter from: "
						+ userCache.get(userIdKey.toString()));

				userId.set(username);
			} else {
				userId.set(userKey);
			}

			outputvalue
					.set("R:" + userId.toString() + ":" + starKey.toString());
			context.write(businessId, outputvalue);
		}

	}

	public static boolean checkReview(double reviewCount) {
		return (reviewCount >= REVIEW_RATING) ? true : false;
	}
}
