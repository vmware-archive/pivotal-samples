package com.pivotal.springdata.jdbc.business;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

@Configuration
@PropertySource("classpath:/jdbc.properties")
public class JdbcConfiguration {
	@Bean(name = "jdbc")
	public JdbcTemplate jdbcTemplate(Environment environment)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		java.sql.Driver driver = (java.sql.Driver) Class.forName(
				environment.getProperty("jdbc.driverClassName")).newInstance();
		String url = environment.getProperty("jdbc.url");
		String user = environment.getProperty("jdbc.username");
		String pw = environment.getProperty("jdbc.password");
		return new JdbcTemplate(new SimpleDriverDataSource(driver, url, user,pw));
	}
}
