package com.pivotal.springdata.jdbc.business;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class BusinessCount {
	public static void main(String args[]) throws Throwable {

		ApplicationContext context = new AnnotationConfigApplicationContext(JdbcConfiguration.class);
		JdbcTemplate jdbcTemplate = (JdbcTemplate) context.getBean("jdbc");
 
		List<City> rowRecords = jdbcTemplate.query(
				"select city,count(*) countOfBusinesses from business group by city",
				new RowMapper<City>() {
					public City mapRow(ResultSet rs, int rowNum) throws SQLException {
						return new City(rs.getString("city"), rs.getInt("countOfBusinesses"));
					}
				});
		System.out.println("Returned: " + rowRecords);
	}
}
