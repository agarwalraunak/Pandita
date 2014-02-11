package com.pandita.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Repository;

import com.pandita.util.HBaseConnectionUtil;
import com.pandita.util.HBaseConnectionUtil.RowData;
import com.pandita.util.MapReduceUtil;

/**
 * 
 * Table: Pandita Column Family: AIRPORT_YEAR_CF Associated Cells: rowID | |
 * Airport::Year Season | | Delay Column Family: YEAR_CF Associated Cells: rowID
 * | | Year dayOfWeek | | Flight Count Airport:FLIGHT_COUNT_COLUMN | | Flight
 * Count Airport:TOTAL_DELAY_COLUMN | | Delay Column Family: AIRLINE_CF
 * Associated Cells: rowID | | Airline Year | | Delay Column Family: AIRPORT_CF
 * Associated Cells: rowID | | Airport
 * 
 * 
 * Nisha's MR: Month | | Av. Delay UNIQUE_CARRIER | | Flight Count Origin:Dest |
 * | Flight Count
 * 
 * @author blackhat
 */
@Repository
public class PanditaDAO {

	private HBaseConnectionUtil hBaseConnectionUtil = HBaseConnectionUtil
			.getInstance();

	private static PanditaDAO panditaDAO;

	private static final Logger log = Logger.getLogger(PanditaDAO.class);

	public static final String TABLE_NAME = "pandita";
	public static final String AIRPORT_YEAR_CF = "airport_year_cf";
	public static final String YEAR_CF = "year_cf";
	public static final String AIRLINE_CF = "airline_cf";
	public static final String FLIGHT_COUNT_COLUMN = "flight_count";
	public static final String TOTAL_DELAY_COLUMN = "total_delay";
	public static final String AIRPORT_CF = "airport_cf";
	private static final String DELIMITER = "::";

	private PanditaDAO() {
	}

	public static PanditaDAO getInstance() {
		if (panditaDAO == null) {
			panditaDAO = new PanditaDAO();
		}
		return panditaDAO;
	}

	private void insertionLogInfo(String columnFamily, String rowID,
			String key, String value) {
		log.info(String
				.format("Inserting Data Column Family --> %s Row ID --> %s Key--> %s Value --> %s",
						columnFamily, rowID, key, value));
	}

	private void insertionLogError(String columnFamily, String rowID,
			String key, String value) {
		log.error(String
				.format("Inserting Data Column Family --> %s Row ID --> %s Key--> %s Value --> %s",
						columnFamily, rowID, key, value));
	}

	/******************************************************************************************
	 *********************** Methods to persist data inside HBase***********************************
	 ****************************************************************************************** 
	 */

	/**
	 * Inserts the data for the <strong>BusyAirportPerYear</strong> MapReduce
	 * Analysis
	 * 
	 * @param {@code String} year
	 * @param {@code String} airport
	 * @param {@code String} flightCount
	 * @return {@code boolean}: true if inserted successfully else false
	 */
	public boolean insertRecordForBusyAirportForEachYear(String year,
			String airport, String flightCount) {

		// Concatenating the airport and the year along with a delimiter to use
		// it as the key of the row
		try {

			insertionLogInfo(AIRPORT_YEAR_CF, year, airport + DELIMITER
					+ FLIGHT_COUNT_COLUMN, flightCount);

			hBaseConnectionUtil.addData(TABLE_NAME, year, AIRPORT_YEAR_CF,
					airport + DELIMITER + FLIGHT_COUNT_COLUMN, flightCount);
		} catch (IOException e) {

			insertionLogError(AIRPORT_YEAR_CF, year, airport + DELIMITER
					+ FLIGHT_COUNT_COLUMN, flightCount);

			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Inserts the data for the <strong>DelayBySeasonPerYear</strong> MapReduce
	 * Analysis
	 * 
	 * @param {@code String} airport
	 * @param {@code String} season
	 * @param {@code String} year
	 * @param {@code String} delay
	 * @return {@code boolean}: True if inserted successfully else false
	 */
	public boolean insertRecordForAirportDelayBySeasonPerYear(String airport,
			String season, String year, String delay) {

		// Concatenating the airport and the year along with a delimiter to use
		// it as the key of the row
		String rowID = new StringBuilder(airport).append(DELIMITER)
				.append(year).toString();

		try {

			insertionLogInfo(AIRPORT_YEAR_CF, rowID, season, delay);

			// Storing delay against the season
			hBaseConnectionUtil.addData(TABLE_NAME, rowID, AIRPORT_YEAR_CF,
					season, delay);
		} catch (IOException e) {

			insertionLogError(AIRPORT_YEAR_CF, rowID, season, delay);

			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Inserts the data for the <strong>DelayPerYear</strong> MapReduce Analysis
	 * 
	 * @param {@code String} airport
	 * @param {@code String} year
	 * @param {@code String} delay
	 * @return {@code boolean}: True if inserted successfully else false
	 */
	public boolean insertRecordForAirportDelayPerYear(String airport,
			String year, String delay) {

		try {
			insertionLogInfo(AIRPORT_YEAR_CF, year, airport + DELIMITER
					+ TOTAL_DELAY_COLUMN, delay);

			hBaseConnectionUtil.addData(TABLE_NAME, year, AIRPORT_YEAR_CF,
					airport + DELIMITER + TOTAL_DELAY_COLUMN, delay);
		} catch (IOException e) {

			insertionLogError(AIRPORT_YEAR_CF, year, airport + DELIMITER
					+ TOTAL_DELAY_COLUMN, delay);

			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Inserts the data for the <strong>FlightCountPerDOWByYear</strong>
	 * MapReduce Analysis
	 * 
	 * @param {@code String} year
	 * @param {@code String} dayOfWeek
	 * @param {@code String} flightCount
	 * @return {@code boolean}: True if inserted successfully else false
	 */
	public boolean insertRecordForFlightCountPerDOWByYear(String year,
			String dayOfWeek, String flightCount) {
		try {

			insertionLogInfo(YEAR_CF, year, dayOfWeek, flightCount);
			hBaseConnectionUtil.addData(TABLE_NAME, year, YEAR_CF, dayOfWeek,
					flightCount);
		} catch (IOException e) {

			insertionLogError(YEAR_CF, year, dayOfWeek, flightCount);

			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Inserts the data for the <strong>AirlinePerformanceByYear</strong>
	 * MapReduce Analysis
	 * 
	 * @param {@code String} year
	 * @param {@code String} airline
	 * @param {@code String} delay
	 * @return {@code boolean}: true if inserted successfully else false
	 */
	public boolean insertRecordForAirlinePerformancePerYear(String year,
			String airline, String delay) {
		try {

			insertionLogInfo(YEAR_CF, airline, year, delay);

			hBaseConnectionUtil.addData(TABLE_NAME, airline, AIRLINE_CF, year,
					delay);
		} catch (IOException e) {

			insertionLogError(YEAR_CF, year, airline, delay);

			e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean insertRecordForAirport(String airport) {
		try {

			insertionLogInfo(AIRLINE_CF, airport, "1", "1");

			hBaseConnectionUtil.addData(TABLE_NAME, airport, AIRLINE_CF, "1",
					"1");
		} catch (IOException e) {

			insertionLogError(YEAR_CF, airport, "1", "1");

			e.printStackTrace();
			return false;
		}
		return true;

	}

	/******************************************************************************************
	 *********************** Methods to retrieve data from HBase***********************************
	 ****************************************************************************************** 
	 */

	/**
	 * Returns the row keys for the given table and column family
	 * 
	 * @param {@code String} tableName: Name of the table
	 * @param {@code String} columnFamily: Name of the column family
	 * @return {@code Set<String>}
	 * @throws IOException
	 */
	public Set<String> retrieveRowKeyForColumnFamilyInTable(String tableName,
			String columnFamily) throws IOException {
		return hBaseConnectionUtil.getRowKeyForColumnFamily(tableName,
				columnFamily);
	}

	/**
	 * Retrieves the data from database for
	 * <strong>BusyAirportForEachYear</strong> MapReduce analysis
	 * 
	 * @param {@code String} startYear
	 * @param {@code String} endYear
	 * @param {@code String[]} airports
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> retrieveBusyAirportForEachYear(
			String startYear, String endYear, String... airports)
			throws IOException {

		String[] airportCF = null;
		if (airports != null) {
			airportCF = new String[airports.length - 1];
			for (int i = 0; i < airports.length; i++) {
				airportCF[i] = airports[i] + FLIGHT_COUNT_COLUMN;
			}
		}

		return hBaseConnectionUtil.getObjectsFromTable(TABLE_NAME,
				AIRPORT_YEAR_CF, startYear, endYear, airportCF);
	}

	/**
	 * Retrieves the data from database for
	 * <strong>AirportDelayBySeasonPerYear</strong> MapReduce analysis
	 * 
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> retrieveAirportDelayBySeasonPerYear()
			throws IOException {
		return hBaseConnectionUtil.getObjectsFromTable(TABLE_NAME,
				AIRPORT_YEAR_CF, null, null, MapReduceUtil.SPRING_SEASON,
				MapReduceUtil.SUMMER_SEASON, MapReduceUtil.WINTER_SEASON,
				MapReduceUtil.FALL_SEASON);
	}

	/**
	 * Retrieves the data from database for <strong>AirportDelayPerYear</strong>
	 * MapReduce analysis
	 * 
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> retrieveAirportDelayPerYear(
			String startYear, String endYear, String... airports)
			throws IOException {

		String[] airportCF = null;
		if (airports != null) {
			airportCF = new String[airports.length - 1];
			for (int i = 0; i < airports.length; i++) {
				airportCF[i] = airports[i] + TOTAL_DELAY_COLUMN;
			}
		}

		return hBaseConnectionUtil.getObjectsFromTable(TABLE_NAME,
				AIRPORT_YEAR_CF, startYear, endYear, airportCF);
	}

	/**
	 * Retrieves the data from database for
	 * <strong>FlightCountPerDOWByYear</strong> MapReduce analysis
	 * 
	 * @param {@code String} startYear
	 * @param {@code String} endYear
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> retrieveFlightCountPerDOWByYear(
			String startYear, String endYear) throws IOException {
		return hBaseConnectionUtil.getObjectsFromTable(TABLE_NAME, YEAR_CF,
				startYear, endYear);
	}

	/**
	 * Retrieves the data from database for
	 * <strong>AirlinePerformancePerYear</strong> MapReduce analysis
	 * 
	 * @param {@code String[]} years: Optional Argument to query for specific
	 *        years else, returns the data for all the years
	 * @return {@code Map<String, Map<String, String>>}
	 * @throws IOException
	 */
	public Map<String, Map<String, String>> retrieveAirlinePerformancePerYear(
			String... years) throws IOException {
		return hBaseConnectionUtil.getObjectsFromTableInMap(TABLE_NAME,
				AIRLINE_CF, null, null, years);
	}

	/**
	 * Returns the flight count for a year along with seasonal delays
	 * 
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> retrieveAirportFlightCountWithSeasonalDelay()
			throws IOException {
		return hBaseConnectionUtil.getObjectsFromTable(TABLE_NAME,
				AIRPORT_YEAR_CF, null, null, FLIGHT_COUNT_COLUMN,
				MapReduceUtil.SPRING_SEASON, MapReduceUtil.SUMMER_SEASON,
				MapReduceUtil.WINTER_SEASON, MapReduceUtil.FALL_SEASON);
	}

	public String retrieveTotalDelayForAirportPerYear(String rowID)
			throws IOException {
		return hBaseConnectionUtil.getValueForRowID(TABLE_NAME,
				AIRPORT_YEAR_CF, rowID, TOTAL_DELAY_COLUMN);
	}
	
}