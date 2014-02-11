/**
 * 
 */
package com.pandita.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.pandita.service.PanditaService;
import com.pandita.util.HBaseConnectionUtil;
import com.pandita.util.HBaseConnectionUtil.RowData;
import com.pandita.util.MapReduceUtil;

/**
 * @author blackhat
 *
 */
public class DBTest {
	
	private @Autowired PanditaService panditaService;
	private @Autowired HBaseConnectionUtil util;
	
	private BufferedReader readFile(String filePath) throws FileNotFoundException{
		FileReader fis = new FileReader(filePath);
		return  new BufferedReader(fis);	
	}
	
	public void insertRecordForBusyAirportForEachYear() throws IOException{
			panditaService.createTable();
		 	BufferedReader br = readFile("/home/blackhat/MR Output/BusyAirportPerYear");
		 	String line;
		 	while((line = br.readLine()) != null){
		 		
		 		String[] keyValue = line.split("\t");
		 		String[] airportYear = keyValue[0].split(MapReduceUtil.MR_DELIMITER);
		 		
		 		panditaService.insertRecordForBusyAirportForEachYear(airportYear[1], airportYear[0], keyValue[1]);
		 	}
	}
	
	public void insertRecordForAirportDelayBySeasonPerYear() throws IOException{
		panditaService.createTable();
	 	BufferedReader br = readFile("/home/blackhat/MR Output/DelayBySeasonPerYear");
	 	String line;
	 	while((line = br.readLine()) != null){
	 		
	 		String[] keyValue = line.split("\t");
	 		String[] airportSeasonYear = keyValue[0].split(MapReduceUtil.MR_DELIMITER);
	 		
	 		panditaService.insertRecordForAirportDelayBySeasonPerYear(airportSeasonYear[0], airportSeasonYear[1], airportSeasonYear[2], keyValue[1]);
	 	}
	}
	
	public void insertRecordForAirportDelayPerYear() throws IOException{
		panditaService.createTable();
	 	BufferedReader br = readFile("/home/blackhat/MR Output/DelayByYear");
	 	String line;
	 	while((line = br.readLine()) != null){
	 		
	 		String[] keyValue = line.split("\t");
	 		String[] airportYear = keyValue[0].split(MapReduceUtil.MR_DELIMITER);
	 		
	 		panditaService.insertRecordForAirportDelayPerYear(airportYear[0], airportYear[1], keyValue[1]);
	 	}
	}
	
	public void insertRecordForFlightCountPerDOWByYear() throws IOException{
		panditaService.createTable();
	 	BufferedReader br = readFile("/home/blackhat/MR Output/FlightCountByDOW");
	 	String line;
	 	while((line = br.readLine()) != null){
	 		
	 		String[] keyValue = line.split("\t");
	 		String[] dowYear = keyValue[0].split(MapReduceUtil.MR_DELIMITER);
	 		
	 		panditaService.insertRecordForFlightCountPerDOWByYear(dowYear[1], dowYear[0], keyValue[1]);
	 	}
	}
	
	public void insertRecordForAirlinePerformancePerYear() throws IOException{
		panditaService.createTable();
	 	BufferedReader br = readFile("/home/blackhat/MR Output/FlightCountByDOW");
	 	String line;
	 	while((line = br.readLine()) != null){
	 		
	 		String[] keyValue = line.split("\t");
	 		String[] airlineYear = keyValue[0].split(MapReduceUtil.MR_DELIMITER);
	 		
	 		panditaService.insertRecordForAirlinePerformancePerYear(airlineYear[1], airlineYear[0], keyValue[1]);
	 	}
	}
	
	public Map<String, ArrayList<RowData>> retrieveAirportFlightCountWithSeasonalDelay() throws IOException{
		return  panditaService.retrieveAirportFlightCountWithSeasonalDelay();
	}

//	public static void main(String[] args) throws IOException {
//		DBTest test = new DBTest();
//		test.insertRecordForBusyAirportForEachYear();
////		test.insertRecordForAirlinePerformancePerYear();
////		test.insertRecordForAirportDelayBySeasonPerYear();
////		test.insertRecordForAirportDelayPerYear();
////		test.insertRecordForFlightCountPerDOWByYear();
////		Map<String, ArrayList<RowData>> output = test.retrieveAirportFlightCountWithSeasonalDelay();
////		HBaseConnectionUtil util = HBaseConnectionUtil.getHBaseConnectionUtil();
////		util.print(output);
//	}	
}