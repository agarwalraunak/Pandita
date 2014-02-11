/**
 * 
 */
package com.pandita.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pandita.dao.PanditaDAO;
import com.pandita.util.HBaseConnectionUtil;
import com.pandita.util.HBaseConnectionUtil.RowData;

/**
 * @author blackhat
 *
 */
@Service
public class PanditaService {
	
	private @Autowired HBaseConnectionUtil util;
	private @Autowired PanditaDAO panditaDAO ;
	
	/**
	 * Creates a table and required column families and returns true if successfull else false
	 * @return {@code boolean}
	 */
	public boolean createTable(){
		try {
			util.createTable(PanditaDAO.TABLE_NAME, PanditaDAO.AIRPORT_YEAR_CF, PanditaDAO.YEAR_CF, PanditaDAO.AIRLINE_CF);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/******************************************************************************************
	 ***********************Methods to persist data inside HBase***********************************									
	 ******************************************************************************************
	 */
	
	/**
	 * @param year
	 * @param airport
	 * @param flightCount
	 * @return
	 */
	public boolean insertRecordForBusyAirportForEachYear(String year, String airport, String flightCount){
		return panditaDAO.insertRecordForBusyAirportForEachYear(year, airport, flightCount);
	}
	
	/**
	 * Inserts the data for the <strong>DelayBySeasonPerYear</strong> MapReduce Analysis
	 * @param {@code String} airport
	 * @param {@code String} season 
	 * @param {@code String} year
	 * @param {@code String} delay
	 * @return {@code boolean}: True if inserted successfully else false
	 */
	public boolean insertRecordForAirportDelayBySeasonPerYear(String airport, String season, String year, String delay){
		return panditaDAO.insertRecordForAirportDelayBySeasonPerYear(airport, season, year, delay);
	}

	/**
	 * Inserts the data for the <strong>DelayPerYear</strong> MapReduce Analysis
	 * @param {@code String} airport
	 * @param {@code String} year
	 * @param {@code String} delay
	 * @return {@code boolean}: True if inserted successfully else false
	 */
	public boolean insertRecordForAirportDelayPerYear(String airport, String year, String delay){
		return panditaDAO.insertRecordForAirportDelayPerYear(airport, year, delay);
	}
	
	/**
	 * Inserts the data for the <strong>FlightCountPerDOWByYear</strong> MapReduce Analysis
	 * @param {@code String} year
	 * @param {@code String} dayOfWeek
	 * @param {@code String} flightCount
	 * @return {@code boolean}: True if inserted successfully else false
	 */
	public boolean insertRecordForFlightCountPerDOWByYear(String year, String dayOfWeek, String flightCount){
		return panditaDAO.insertRecordForFlightCountPerDOWByYear(year, dayOfWeek, flightCount);
	}
	
	/**
	 * Inserts the data for the <strong>AirlinePerformanceByYear</strong> MapReduce Analysis
	 * @param {@code String} year
	 * @param {@code String} airline
	 * @param {@code String} delay
	 * @return {@code boolean}: true if inserted successfully else false
	 */
	public boolean insertRecordForAirlinePerformancePerYear(String year, String airline, String delay){
		return panditaDAO.insertRecordForAirlinePerformancePerYear(year, airline, delay);
	}
	
	public boolean insertRecordForAirport(String airport) {
		return panditaDAO.insertRecordForAirport(airport);
	}
	
	/******************************************************************************************
	 ***********************Methods to retrieve data from HBase***********************************									
	 ******************************************************************************************
	 */
	
	/**
	 * Returns the row keys for the given table and column family
	 * @param {@code String} tableName: Name of the table
	 * @param {@code String} columnFamily: Name of the column family
	 * @return {@code Set<String>}
	 * @throws IOException
	 */
	public Set<String> retrieveRowKeyForAirlineColumnFamily() {
		try {
			return panditaDAO.retrieveRowKeyForColumnFamilyInTable(PanditaDAO.TABLE_NAME, PanditaDAO.AIRLINE_CF);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Returns the row keys for the given table and column family
	 * @param {@code String} tableName: Name of the table
	 * @param {@code String} columnFamily: Name of the column family
	 * @return {@code Set<String>}
	 * @throws IOException
	 */
	public Set<String> retrieveRowKeyForAirportColumnFamily() {
		try {
			return panditaDAO.retrieveRowKeyForColumnFamilyInTable(PanditaDAO.TABLE_NAME, PanditaDAO.AIRPORT_CF);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public Map<String, Map<String, String>> retrieveAirlinePerformancePerYear(String... years){
		try {
			return panditaDAO.retrieveAirlinePerformancePerYear(years);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Returns the flight count for a year along with seasonal delays
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> retrieveAirportFlightCountWithSeasonalDelay() throws IOException{
		return panditaDAO.retrieveAirportFlightCountWithSeasonalDelay();
	}
	
	public Map<String, ArrayList<RowData>> retrieveBusyAirportForEachYear(String startYear, String  endYear, String... airports) throws IOException{
		return panditaDAO.retrieveBusyAirportForEachYear(startYear, endYear, airports);
	}
	
	/**
	 * Retrieves the data from database for <strong>AirportDelayPerYear</strong> MapReduce analysis 
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> retrieveAirportDelayPerYear(String startYear, String endYear, String... airports) throws IOException{
		return panditaDAO.retrieveAirportDelayPerYear(startYear, endYear, airports);
	}
}