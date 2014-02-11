/**
 * 
 */
package com.pandita.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

/**
 * @author raunak
 * 
 */
@Component
public class HBaseConnectionUtil {

	private static HBaseConnectionUtil hBaseConnectionUtil;

	private static Configuration conf;
	private static HTablePool pool;

	private HBaseConnectionUtil() {
	}

	static {
		// By default creates the configuration for localhost
		conf = HBaseConfiguration.create();
		// Without pooling, the connection to a table will be reinitialized.
		// Creating a new connection to a table might take up to 5-10 seconds!
		pool = new HTablePool(conf, 10);

	}

	public static HBaseConnectionUtil getInstance() {
		if (hBaseConnectionUtil == null) {
			hBaseConnectionUtil = new HBaseConnectionUtil();
		}
		return hBaseConnectionUtil;
	}

	public static class RowData {

		private String key;
		private String value;

		/**
		 * @return the key
		 */
		public String getKey() {
			return key;
		}

		/**
		 * @param key
		 *            the key to set
		 */
		public void setKey(String key) {
			this.key = key;
		}

		/**
		 * @return the value
		 */
		public String getValue() {
			return value;
		}

		/**
		 * @param value
		 *            the value to set
		 */
		public void setValue(String value) {
			this.value = value;
		}
	}

	/**
	 * Creates a HBase Table for the given table name and column names
	 * 
	 * @param {@code String} tableName: Name of the table to be created
	 * @param {@code String} columnFamilyNames: Name of the columns families to
	 *        be created
	 * @throws IOException
	 */
	public void createTable(String tableName, String... columnFamilyNames)
			throws IOException {

		HBaseAdmin admin = new HBaseAdmin(conf);

		// If the table with the given table name exists do nothing
		if (!admin.tableExists(tableName)) {

			// Creating a table
			HTableDescriptor table = new HTableDescriptor(tableName);
			admin.createTable(table);

			// Edit operations can not be performed on an active table. So,
			// deactivate it
			admin.disableTable(tableName);

			// Adding the passed in columns to the table
			for (String columnFamily : columnFamilyNames) {

				HColumnDescriptor columnDesc = new HColumnDescriptor(
						columnFamily);
				admin.addColumn(tableName, columnDesc);

			}

			// Reactivate the table for reading operations
			admin.enableTable(tableName);
		}
		// Close the HBase Administrator once the operation is complete
		admin.close();
	}

	public String getValueForRowID(String tableName, String columnFamily,
			String rowID, String key) throws IOException {

		if (!validateStringIfNullOrEmpty(tableName, rowID, key)) {
			throw new IllegalArgumentException(
					"Invalid tableName input to HBaseConnectionUtil.getValueForRowID");
		}

		HTableInterface table = getHTable(tableName);
		Get g = new Get(Bytes.toBytes(rowID));
		Result r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes(columnFamily),
				Bytes.toBytes(key));
		return value == null ? null : new String(value);
	}

	/**
	 * Returns the row keys for the given table and column family
	 * 
	 * @param {@code String} tableName: Name of the table
	 * @param {@code String} columnFamily: Name of the column family
	 * @return {@code Set<String>}
	 * @throws IOException
	 */
	public Set<String> getRowKeyForColumnFamily(String tableName,
			String columnFamily) throws IOException {

		// Scan object to loop through the Table to find the objects
		Scan scan = new Scan();
		byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
		scan.addFamily(columnFamilyBytes);

		// Get the table object refered by the tableName
		HTableInterface table = getHTable(tableName);

		Set<String> rowKeys = new HashSet<String>();

		// Initiate the Scanning of the Table
		ResultScanner scanner = table.getScanner(scan);

		for (Result result : scanner) {
			// Extracting the KeyValue pair for each row inside the column
			// family
			for (KeyValue keyValue : result.raw()) {
				rowKeys.add(Bytes.toString(keyValue.getRow()));
			}
		}

		return rowKeys;
	}

	/**
	 * Utility method to fetch data for the given parameters from HBase
	 * 
	 * @param {@code String} tableName: Name of the Table from which data has to
	 *        be extracted
	 * @param {@code String} columnFamily: Column Family from which data has to
	 *        be extracted
	 * @param {@code String} startRange: Optional attribute to perform a range
	 *        query
	 * @param {@code String} endRange: Optional attribute to perform a range
	 *        query
	 * @param {@code String[]} retriveColumns: Optional Argument, takes the name
	 *        of the columns to be retrieved from each row
	 * @return {@code Map<String, ArrayList<RowData>>}
	 * @throws IOException
	 */
	public Map<String, ArrayList<RowData>> getObjectsFromTable(
			String tableName, String columnFamily, String startRange,
			String endRange, String... retrieveColumns) throws IOException {

		// Validate the tableName
		if (!validateStringIfNullOrEmpty(tableName)) {
			throw new IllegalArgumentException(
					"Invalid tableName input to HBaseConnectionUtil.getObjectsFromTable");
		}

		// Scan object to loop through the Table to find the objects
		Scan scan = new Scan();
		byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
		scan.addFamily(columnFamilyBytes);

		// Add the filter for columns to be retrieved
		if (retrieveColumns != null && retrieveColumns.length > 0) {
			for (String columnName : retrieveColumns)
				scan.addColumn(columnFamilyBytes, Bytes.toBytes(columnName));
		}

		// Get the table object refered by the tableName
		HTableInterface table = getHTable(tableName);

		// Check if the startRange and endRange are given
		// If yes, set the start and end range for the scan object created above
		if (validateStringIfNullOrEmpty(startRange, endRange)) {
			scan.setStartRow(Bytes.toBytes(startRange));
			scan.setStopRow(Bytes.toBytes(endRange));
		}

		// Create a Data Structure to store the data from the table
		// Key for the Map is a String i.e. the rowID for the given table
		// Value is an ArrayList of RowData. RowData is going to be used to save
		// the
		// Key-Value pairs inside the column family
		Map<String, ArrayList<RowData>> resultSet = new HashMap<String, ArrayList<RowData>>();

		// Initiate the Scanning of the Table
		ResultScanner scanner = table.getScanner(scan);

		RowData rowData = null;
		String rowId = null;
		ArrayList<RowData> mapValue;

		// Looping through the Scanned Result
		for (Result result : scanner) {
			// Extracting the KeyValue pair for each row inside the column
			// family
			for (KeyValue keyValue : result.raw()) {
				rowData = new RowData();
				rowData.setKey(Bytes.toString(keyValue.getQualifier()));
				rowData.setValue(Bytes.toString(keyValue.getValue()));
				rowId = Bytes.toString(result.getRow());

				// Check if the Map contains the rowID
				// if yes, re-use the corresponding ArrayList to store the new
				// RowData object
				// or else create a new one
				if (resultSet.containsKey(rowId)) {
					mapValue = resultSet.get(rowId);
				} else {
					mapValue = new ArrayList<RowData>();
				}

				// Storing the new RowData object inside the ArrayList
				mapValue.add(rowData);

				// Saving it inside the Map
				resultSet.put(rowId, mapValue);
			}
		}
		return resultSet;
	}

	/**
	 * Utility method to fetch data for the given parameters from HBase
	 * 
	 * @param {@code String} tableName: Name of the Table from which data has to
	 *        be extracted
	 * @param {@code String} columnFamily: Column Family from which data has to
	 *        be extracted
	 * @param {@code String} startRange: Optional attribute to perform a range
	 *        query
	 * @param {@code String} endRange: Optional attribute to perform a range
	 *        query
	 * @param {@code String[]} retriveColumns: Optional Argument, takes the name
	 *        of the columns to be retrieved from each row
	 * @return {@code Map<String, Map<String, String>>}
	 * @throws IOException
	 */
	public Map<String, Map<String, String>> getObjectsFromTableInMap(
			String tableName, String columnFamily, String startRange,
			String endRange, String... retrieveColumns) throws IOException {

		// Validate the tableName
		if (!validateStringIfNullOrEmpty(tableName)) {
			throw new IllegalArgumentException(
					"Invalid tableName input to HBaseConnectionUtil.getObjectsFromTable");
		}

		// Scan object to loop through the Table to find the objects
		Scan scan = new Scan();
		byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
		scan.addFamily(columnFamilyBytes);

		// Add the filter for columns to be retrieved
		if (retrieveColumns != null && retrieveColumns.length > 0) {
			for (String columnName : retrieveColumns)
				scan.addColumn(columnFamilyBytes, Bytes.toBytes(columnName));
		}

		// Get the table object refered by the tableName
		HTableInterface table = getHTable(tableName);

		// Check if the startRange and endRange are given
		// If yes, set the start and end range for the scan object created above
		if (validateStringIfNullOrEmpty(startRange, endRange)) {
			scan.setStartRow(Bytes.toBytes(startRange));
			scan.setStopRow(Bytes.toBytes(endRange));
		}

		// Create a Data Structure to store the data from the table
		// Key for the Map is a String i.e. the rowID for the given table
		// Value is an ArrayList of RowData. RowData is going to be used to save
		// the
		// Key-Value pairs inside the column family
		Map<String, Map<String, String>> resultSet = new HashMap<String, Map<String, String>>();

		// Initiate the Scanning of the Table
		ResultScanner scanner = table.getScanner(scan);

		RowData rowData = null;
		String rowId = null;
		Map<String, String> mapValue;

		String key, value;
		// Looping through the Scanned Result
		for (Result result : scanner) {
			// Extracting the KeyValue pair for each row inside the column
			// family
			for (KeyValue keyValue : result.raw()) {
				key = Bytes.toString(keyValue.getQualifier());
				value = Bytes.toString(keyValue.getValue());
				rowId = Bytes.toString(result.getRow());

				// Check if the Map contains the rowID
				// if yes, re-use the corresponding ArrayList to store the new
				// RowData object
				// or else create a new one
				if (resultSet.containsKey(rowId)) {
					mapValue = resultSet.get(rowId);
				} else {
					mapValue = new HashMap<String, String>();
				}

				// Storing the new RowData object inside the ArrayList
				mapValue.put(key, value);

				// Saving it inside the Map
				resultSet.put(rowId, mapValue);
			}
		}
		return resultSet;
	}
	
	/**
	 * Adds data for given input parameter
	 * 
	 * @param {@code String} tableName: Name of the table where data has to be
	 *        inserted
	 * @param {@code String} rowID: ID of the row to be created or where entry
	 *        has to be added
	 * @param {@code String} columnFamily: Name of the column family where the
	 *        data has to be added
	 * @param {@code String} key: Name of the Column
	 * @param {@code String} value: Value for the column
	 * @throws IOException
	 */
	public void addData(String tableName, String rowID, String columnFamily,
			String key, String value) throws IOException {

		if (!validateStringIfNullOrEmpty(tableName, rowID, columnFamily, key,
				value)) {
			throw new IllegalArgumentException(
					"Invlid input parameter to HBaseConnectionUtil.addData method");
		}

		// Get the Table for the given table name
		HTableInterface table = getHTable(tableName);

		// Creating a row to be inserted in the table
		Put row = new Put(Bytes.toBytes(rowID));

		// Adding the attributes to the row
		row.add(Bytes.toBytes(columnFamily), // Column Family
				Bytes.toBytes(key), // Key for the cell
				Bytes.toBytes(value) // Value for the cell
		);

		// Adding the row to the table
		table.put(row);

		table.close();
	}

	// public static void main(String[] args) throws IOException {
	//
	// HBaseConnectionUtil util = HBaseConnectionUtil.getHBaseConnectionUtil();
	//
	//
	// System.out.println(util.getValueForRowID("pandita", PanditaDAO.YEAR_CF,
	// "1987", "1"));
	//
	// // util.createTable("raunak", "agarwal", "jindal");
	// //
	// // util.addData("raunak", "1", "agarwal", "key1", "updated_value1");
	// // util.addData("raunak", "2", "agarwal", "agarwal_key2",
	// "updated_agarwal_value2");
	// // util.addData("raunak", "3", "agarwal", "agarwal_key3",
	// "updated_agarwal_value3");
	// // util.addData("raunak", "4", "agarwal", "agarwal_key4",
	// "updated_agarwal_value4");
	// // util.addData("raunak", "5", "agarwal", "agarwal_key5",
	// "updated_agarwal_value5");
	// // util.addData("raunak", "6", "agarwal", "agarwal_key6",
	// "updated_agarwal_value6");
	// //
	// //
	// // util.addData("raunak", "2", "jindal", "key2", "value2");
	// //
	// // Map<String, ArrayList<RowData>> result =
	// util.getObjectsFromTable("raunak", "agarwal", "1", "2", "agarwal_key4",
	// "agarwal_key6");
	// // util.print(result);
	// // result = util.getObjectsFromTable("raunak", "agarwal", null, null);
	// // util.print(result);
	// }

	public void print(Map<String, ArrayList<RowData>> result) {

		Iterator<String> iterator = result.keySet().iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			ArrayList<RowData> data = result.get(key);
			System.out.println("RowKey --> " + key);
			for (RowData r : data) {
				System.out.println("Key: " + r.getKey() + " :: value: "
						+ r.getValue());
			}
		}
	}

	private HTableInterface getHTable(String tableName) {
		return pool.getTable(tableName);
	}

	private boolean validateStringIfNullOrEmpty(String... input) {
		if (input == null || input.length == 0) {
			return false;
		}
		for (String s : input) {
			if (s == null || s.trim().isEmpty()) {
				return false;
			}
		}
		return true;
	}

}