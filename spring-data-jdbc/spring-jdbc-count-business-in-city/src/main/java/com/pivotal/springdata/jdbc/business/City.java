package com.pivotal.springdata.jdbc.business;

public class City {
	private int countOfBusinesses;
	private String cityName;

	public City(String name, int count) {
		this.cityName = name;
		this.countOfBusinesses = count;
	}

	public String getCityName() {
		return cityName;
	}

	public int getCountOfBusinesses() {
		return countOfBusinesses;
	}
	
	 public String toString() {
         String s = "{" +
                "city=" + cityName + "," +
                "count=" + countOfBusinesses + 
                 "}";
         return s;
     }
}