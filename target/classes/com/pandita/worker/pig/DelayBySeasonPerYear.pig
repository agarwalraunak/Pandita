REGISTER pandita.jar;
data = LOAD '1987-data-set.csv' USING PigStorage(',') AS 
(Year,Month:int,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,
	FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay:int,DepDelay,Origin,
	Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,
	WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay);
A = FILTER data BY (ArrDelay > 0);
X = GROUP A BY (Dest, Year, 
		(
			Case
				when Month>2 AND Month<6 THEN 'SPRING'
				when Month>5 AND Month<9 THEN 'SUMMER'
				when Month>8 AND Month<12 THEN 'FALL'
				when Month==12 OR (Month<3 AND Month>0) THEN 'WINTER'
			END
		)
	);
Y = FOREACH X GENERATE group.Dest, group.Year, group.$2, SUM(A.ArrDelay);
STORE Y INTO 'myoutput';