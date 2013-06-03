package com.pivotal.springdata.jdbc.business;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class ListFiveStarBusinessReviewers {
	ApplicationContext context;
	DataSource dataSource;
	JdbcTemplate template;

	public ListFiveStarBusinessReviewers() {
		context = new ClassPathXmlApplicationContext("datasources-beans.xml");
		dataSource = context.getBean("postGresDataSource", DataSource.class);
		template = new JdbcTemplate(dataSource);
	}

	public void executeQuery() {
		List<RowRecord> rowRecords = template
				.query("select name,user_id FROM business JOIN review  ON (business.business_id=review.business_id AND review.stars=5.0) ",
						new RowMapper<RowRecord>() {
							public RowRecord mapRow(ResultSet rs, int rowNum)
									throws SQLException {
								RowRecord row = new RowRecord();
								row.name = rs.getString("name");
								row.user_id = rs.getString("user_id");
								return row;
							}
						});
		System.out.println("Returned: " + rowRecords);
	}

	class RowRecord {
		String name;
		String user_id;

		public String toString() {
			String s = "{" + "name=" + name + "," + "user_id=" + user_id + "}";
			return s;
		}
	}

	public static void main(String[] args) {
		ListFiveStarBusinessReviewers app = new ListFiveStarBusinessReviewers();
		app.executeQuery();
	}
}
