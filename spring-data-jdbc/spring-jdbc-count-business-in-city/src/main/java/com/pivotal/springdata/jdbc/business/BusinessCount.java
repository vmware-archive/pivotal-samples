package com.pivotal.springdata.jdbc.business;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class BusinessCount {
    ApplicationContext context;
    DataSource dataSource;
    JdbcTemplate template;
    
    public BusinessCount() {
        context = new ClassPathXmlApplicationContext("datasources-beans.xml");
        dataSource = context.getBean("hawqDataSource", DataSource.class);
        template = new JdbcTemplate(dataSource);
    }
    
    public void executeQuery() {
        List<RowRecord> rowRecords = template.query(
                "select city,count(*) a from business group by city",
                new RowMapper<RowRecord>() {
                    public RowRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
                        RowRecord row = new RowRecord();
                        row.city = rs.getString("city");
                        row.count = rs.getInt("a");
                        return row;
                    }
                });
        System.out.println("Returned: "+ rowRecords);
    }
    
    class RowRecord {
        String city;
        int count;
        public String toString() {
            String s = "{" +
                   "city=" + city + "," +
                   "count=" + count + 
                    "}";
            return s;
        }
    }
    public static void main( String[] args ){
        BusinessCount app = new BusinessCount();
        app.executeQuery();    
    }
}
