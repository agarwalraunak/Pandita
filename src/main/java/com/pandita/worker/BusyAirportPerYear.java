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
public class BusyAirportPerYear implements IBaseMapReduce{
	
	public static class BusyAirportMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{

		private Text mapperKey = new Text();
		private IntWritable mapperValue = new IntWritable(1);
		private MapReduceUtil mapReduceUtil = new MapReduceUtil();
		
		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			String[] values = value.toString().split(",");
			
			String origin = values[MapReduceUtil.ORIGIN];
			String year = values[MapReduceUtil.YEAR];
			
			if (!mapReduceUtil.checkIfFileHeader(year)){
				return;
			}
			
			String mapperKeyStr = mapReduceUtil.mergeStringWithDelimiter(origin, year);
			mapperKey.set(mapperKeyStr);
			output.collect(mapperKey, mapperValue);
			
		}
	}

	public static class BusyAirportReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable reduceValue = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			int sum = 0;
			while(values.hasNext()){
				sum +=	values.next().get();
			}
			reduceValue.set(sum);
			output.collect(key, reduceValue);
			
		}
		
	}
	
	@Override
	public JobConf configureMapReduceJob(){
		JobConf job = new JobConf(BusyAirportPerYear.class);
		job.setJobName("Busy Airport Per Year");
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(BusyAirportMapper.class);
		job.setCombinerClass(BusyAirportReducer.class);
		job.setReducerClass(BusyAirportReducer.class);
		
		return job;
	}

}
