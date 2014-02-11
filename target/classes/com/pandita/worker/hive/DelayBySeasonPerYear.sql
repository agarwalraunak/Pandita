DROP TABLE pandita;

CREATE EXTERNAL TABLE pandita(
					Year INT,
					Month INT,
					DayofMonth INT,
					DayOfWeek INT,
					DepTime INT,
					CRSDepTime INT,
					ArrTime INT,
					CRSArrTime INT,
					UniqueCarrier STRING,
					FlightNum STRING,
					TailNum STRING,
					ActualElapsedTime STRING,
					CRSElapsedTime STRING,
					AirTime INT,
					ArrDelay INT,
					DepDelay INT,
					Origin STRING,
					Dest STRING,
					Distance STRING,
					TaxiIn STRING,
					TaxiOut STRING,
					Cancelled STRING,
					CancellationCode STRING,
					Diverted STRING,
					CarrierDelay STRING,
					WeatherDelay STRING,
					NASDelay STRING,
					SecurityDelay STRING,
					LateAircraftDelay STRING
					)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '44';

LOAD DATA INPATH '/home/blackhat/' OVERWRITE INTO TABLE pandita;

INSERT OVERWRITE DIRECTORY 's3://airlinedatasetmroutput/hive/'
SELECT 
	Dest, 
	Year, 
	CASE 	WHEN Month>2 AND Month<6 THEN 'SPRING'
			WHEN Month>5 AND Month<9 THEN 'SUMMER'
			WHEN Month>8 AND Month<12 THEN 'FALL'
			WHEN Month==12 OR (Month<3 AND Month>0) THEN 'WINTER'
	END,
	SUM(ArrDelay)
 	from pandita WHERE ArrDelay>0 GROUP BY Dest, Year, CASE 	WHEN Month>2 AND Month<6 THEN 'SPRING'
			WHEN Month>5 AND Month<9 THEN 'SUMMER'
			WHEN Month>8 AND Month<12 THEN 'FALL'
			WHEN Month==12 OR (Month<3 AND Month>0) THEN 'WINTER'
	END;