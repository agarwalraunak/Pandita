package com.pandita.util;

/**
 * Helper class with the basic utility methods requried for writing Map Reduce jobs
 * 
 * @author blackhat
 *
 */
public class MapReduceUtil {
	
	
	public static final int YEAR = 0;
	public static final int MONTH = 1;
	public static final int DAY_OF_MONTH = 2;
	public static final int DAY_OF_WEEK = 3;
	public static final int DEPT_TIME = 4;
	public static final int CRS_DEPT_TIME = 5;
	public static final int ARR_TIME = 6;
	public static final int CRS_ARR_TIME= 7;
	public static final int UNIQUE_CARRIER = 8;
	public static final int FLIGHT_NUM = 9;
	public static final int TAIL_NUM = 10;
	public static final int ACTUAL_ELAPSED_TIME = 11;
	public static final int CRS_ELAPSED_TIME= 12;
	public static final int AIR_TIME= 13;
	public static final int ARR_DELAY = 14;
	public static final int DEP_DELAY = 15;
	public static final int ORIGIN = 16;
	public static final int DEST = 17;
	public static final int DISTANCE = 18;
	public static final int TAXI_IN = 19;
	public static final int TAXI_OUT = 20;
	public static final int CANCELLED = 21;
	public static final int CANCELLATION_CODE = 22;
	public static final int DIVERTED = 23;
	public static final int CARRIER_DELAY= 24;
	public static final int WEATHER_DELAY = 25;
	public static final int NAS_DELAY = 26;
	public static final int SECURITY_DELAY = 27;
	public static final int LATE_AIRCRAFT_DELAY = 28;
	
	public static final String MR_DELIMITER = ":";
	public  static final String SPRING_SEASON = "SPRING";
	public  static final String SUMMER_SEASON = "SUMMER";
	public  static final String FALL_SEASON = "FALL";
	public static final String WINTER_SEASON = "WINTER";
	
	public String mergeStringWithDelimiter(String... values){
		
		if (values == null){
			throw new IllegalArgumentException("Invalid input to mergeStringWithDelimiter method");
		}
		
		if (values.length == 0){
			return null;
		}
		
		StringBuilder builder = new StringBuilder();
		for (int i=0; i < values.length; i++){
			builder.append(values[i]);
			if (i != values.length-1)
				builder.append(MR_DELIMITER);
		}	
		return builder.toString();
	}
	
	public String getSeasonForMonth(String monthStr){
		if (monthStr == null || monthStr.trim().isEmpty()){
			throw new IllegalArgumentException("Invalid input to getSeasonForMonth method");
		}
		
		int month;
		try{
			month = Integer.parseInt(monthStr);
		} catch(NumberFormatException e){
			e.printStackTrace();
			return null;
		}
		
		String season = null;
		if (month > 2 && month < 6){
			season = SPRING_SEASON;
		}
		else if (month > 5 && month < 9){
			season = SUMMER_SEASON;
		}
		else if (month > 8 && month < 12){
			season = FALL_SEASON;
		}
		else if (month == 12 || (month < 3 && month > 0)){
			season = WINTER_SEASON;
		}
		
		return season;
	}
	
	public boolean checkIfFileHeader(String year){
		try{
			Integer.parseInt(year);
		}
		catch(NumberFormatException e){
			return false;
		}
		return true;
	}
}