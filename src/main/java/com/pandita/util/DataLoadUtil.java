/**
 * 
 */
package com.pandita.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.pandita.service.PanditaService;

/**
 * @author blackhat
 * 
 */
@Component
public class DataLoadUtil {

	private @Autowired	PanditaService panditaService;

//	@RequestMapping("insertRecordForAirlinePerformancePerYear.htm")
//	public @ResponseBody
//	String insertRecordForAirlinePerformancePerYear() throws IOException {
//		panditaService.createTable();
//
//		String basePath = "/airlineperformanceperyear/part-0000";
//		String[] filePaths = generatePaths(basePath, 8);
//
//		for (String path : filePaths) {
//			BufferedReader br = readFile(path);
//			String line;
//			while ((line = br.readLine()) != null) {
//
//				String[] keyValue = line.split("\t");
//				String[] airlineYear = keyValue[0]
//						.split(MapReduceUtil.MR_DELIMITER);
//				panditaService.insertRecordForAirlinePerformancePerYear(
//						airlineYear[1], airlineYear[0], keyValue[1]);
//			}
//		}
//		return "insertRecordForAirlinePerformancePerYear sucessful";
//	}

//	@RequestMapping("insertRecordForBusyAirportPerYear.htm")
//	public @ResponseBody
//	String insertRecordForBusyAirportPerYear() throws IOException {
//		panditaService.createTable();
//
//		String basePath = "/busyairportperyear/part-0000";
//		String[] filePaths = generatePaths(basePath, 8);
//
//		for (String path : filePaths) {
//			BufferedReader br = readFile(path);
//			String line;
//			while ((line = br.readLine()) != null) {
//
//				String[] keyValue = line.split("\t");
//				String[] airlineYear = keyValue[0]
//						.split(MapReduceUtil.MR_DELIMITER);
//				panditaService.insertRecordForBusyAirportForEachYear(
//						airlineYear[1], airlineYear[0], keyValue[1]);
//			}
//		}
//
//		return "insertRecordForBusyAirportPerYear success";
//	}
//
//	@RequestMapping("insertRecordForAirportDelayPerYear.htm")
//	public @ResponseBody
//	String insertRecordForAirportDelayPerYear() throws IOException {
//		panditaService.createTable();
//
//		String basePath = "/delayperyear/part-0000";
//		String[] filePaths = generatePaths(basePath, 8);
//
//		for (String path : filePaths) {
//			BufferedReader br = readFile(path);
//			String line;
//			while ((line = br.readLine()) != null) {
//
//				String[] keyValue = line.split("\t");
//				String[] airlineYear = keyValue[0]
//						.split(MapReduceUtil.MR_DELIMITER);
//				panditaService.insertRecordForAirportDelayPerYear(
//						airlineYear[0], airlineYear[1], keyValue[1]);
//			}
//		}
//
//		return "insertRecordForAirportDelayPerYear success";
//	}
//
//	@RequestMapping("insertRecordForAirport.htm")
//	public @ResponseBody
//	String insertRecordForAirport() throws IOException {
//
//		panditaService.createTable();
//
//		String basePath = "/delayperyear/part-0000";
//		String[] filePaths = generatePaths(basePath, 8);
//
//		for (String path : filePaths) {
//			BufferedReader br = readFile(path);
//			String line;
//			while ((line = br.readLine()) != null) {
//
//				String[] keyValue = line.split("\t");
//				String[] airportYear = keyValue[0]
//						.split(MapReduceUtil.MR_DELIMITER);
//				panditaService.insertRecordForAirport(airportYear[0]);
//			}
//		}
//
//		return "insertRecordForAirport success";
//	}

	private String[] generatePaths(String basePath, int size) {
		String[] filePaths = new String[size];
		for (int i = 0; i < size; i++) {
			filePaths[i] = basePath + i;
		}
		return filePaths;
	}

	public BufferedReader readFile(String filePath)
			throws FileNotFoundException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(filePath)));
		return reader;
	}

}
