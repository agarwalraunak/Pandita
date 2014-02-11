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
public class FlightCountByDOW implements IBaseMapReduce{
	
	
	public static class FlightCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{

		private IntWritable mapperValue = new IntWritable(1); 
		private MapReduceUtil mapReduceUtil = new MapReduceUtil();
		
		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			String[] values = value.toString().split(",");
			
			String year = values[MapReduceUtil.YEAR];
			String dayOfWeek = values[MapReduceUtil.DAY_OF_WEEK];
			
			if (!mapReduceUtil.checkIfFileHeader(year)){
				return;
			}

			String mapperKeyStr = mapReduceUtil.mergeStringWithDelimiter(dayOfWeek, year);
			
			Text mapperKey = new Text();
			mapperKey.set(mapperKeyStr);
			output.collect(mapperKey, mapperValue);
		}
		
	}
	
	public static class FlightCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable summation = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {

			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			summation.set(sum);
			
			output.collect(key, summation);
		}
	}

	@Override
	public JobConf configureMapReduceJob(){
		JobConf job = new JobConf(FlightCountByDOW.class);
		job.setJobName("Flight Count By Day of Week");
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(FlightCountMapper.class);
		job.setCombinerClass(FlightCountReducer.class);
		job.setReducerClass(FlightCountReducer.class);
		
		return job;
	}

	
}
