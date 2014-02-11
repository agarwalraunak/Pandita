/**
 * 
 */
package com.pandita.worker;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.pandita.util.MapReduceUtil;

/**
 * @author blackhat
 *
 */
public class AirlinePerformanceByYear implements IBaseMapReduce{

	public static final String AIRLINES_TOTAL_DELAY = "AIRLINES_TOTAL_DELAY";
	
	public static class AirlinePerformanceMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{

		private Text mapperKey = new Text();
		private IntWritable mapperValue = new IntWritable();
		private MapReduceUtil mapReduceUtil = new MapReduceUtil();
		
		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		
			String[] values = value.toString().split(",");
			
			String airline = values[MapReduceUtil.UNIQUE_CARRIER];
			String year = values[MapReduceUtil.YEAR];
			String arrivalDelayStr = values[MapReduceUtil.ARR_DELAY];
			
			if (!mapReduceUtil.checkIfFileHeader(year)){
				return;
			}
			
			int arrivalDelay;
			try{
				arrivalDelay = Integer.parseInt(arrivalDelayStr);
				arrivalDelay = arrivalDelay > 0 ? arrivalDelay : 0;
			} catch(NumberFormatException e){
				return;
			}
			
			if (arrivalDelay <= 0){
				return;
			}
			
			String mapperKeyStr = mapReduceUtil.mergeStringWithDelimiter(airline, year);
			mapperKey.set(mapperKeyStr);
			mapperValue.set(arrivalDelay);
			
			output.collect(mapperKey, mapperValue);
			
			mapperKeyStr = mapReduceUtil.mergeStringWithDelimiter(AIRLINES_TOTAL_DELAY, year);
			mapperKey.set(mapperKeyStr);
			output.collect(mapperKey, mapperValue);
		}
	}

	public static class AirlinePerformanceReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable reduceValue = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			//If the key for the reduce call is TOTAL_DELAY
			//Execute the block of code to calculate the total delay for all the airlines for the year
			if (key.toString().contains(AIRLINES_TOTAL_DELAY)){
				int airlinesTotalDelay = 0;
				while(values.hasNext()){
					airlinesTotalDelay+= values.next().get();
				}
				reduceValue.set(airlinesTotalDelay);
				output.collect(key, reduceValue);
				return;
			}
			
			int totalAirlineDelay = 0;
			while(values.hasNext()){
				totalAirlineDelay+= values.next().get();
			}
			reduceValue.set(totalAirlineDelay);
			output.collect(key, reduceValue);
		}
	}
	
	@Override
	public JobConf configureMapReduceJob(){
		JobConf job = new JobConf(AirlinePerformanceByYear.class);
		job.setJobName("Airline Performance Per Year");
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(AirlinePerformanceMapper.class);
		job.setCombinerClass(AirlinePerformanceReducer.class);
		job.setReducerClass(AirlinePerformanceReducer.class);
		
		return job;
	}
}
