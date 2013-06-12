package com.pivotal.springdata.jdbc.business;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class PostalZip {
	ApplicationContext context;
	DataSource dataSource;
	JdbcTemplate template;

	public PostalZip() {
		context = new ClassPathXmlApplicationContext("datasources-beans.xml");
		dataSource = context.getBean("postGresDataSource", DataSource.class);
		template = new JdbcTemplate(dataSource);
	}

	public void executeQuery() {
		List<RowRecord> rowRecords = template
				.query(

				"select billing_address_postal_code, sum(total_paid_amount::float8) as total, sum(total_tax_amount::float8) as tax from retail_demo.orders_hawq group by billing_address_postal_code order by total desc limit 10;",

				new RowMapper<RowRecord>() {
					public RowRecord mapRow(ResultSet rs, int rowNum)
							throws SQLException {
						RowRecord row = new RowRecord();

						row.billing_address_postal_code = rs
								.getString("billing_address_postal_code");
						row.total = rs.getInt("total");
						row.tax = rs.getFloat("tax");
						return row;
					}
				});
		System.out.println("Returned: " + rowRecords);
	}

	class RowRecord {
		String billing_address_postal_code;
		int total;
		float tax;

		public String toString() {
			String s = "{" +

			"total=" + total + "}" + "{" +

			"billing_address_postal_code=" + billing_address_postal_code + "}"
					+ "{" +

					"tax=" + tax + "}";
			return s;
		}
	}

	public static void main(String[] args) {
		PostalZip app = new PostalZip();
		app.executeQuery();
	}
}
