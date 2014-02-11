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
public class DelayBySeasonPerYear implements IBaseMapReduce {
	

	public static class DelayBySeasonMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{

		private Text mapperKey = new Text();
		private IntWritable mapperValue = new IntWritable();
		private MapReduceUtil mapReduceUtil = new MapReduceUtil();
		
		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			String[] values = value.toString().split(",");
			
			String destination = values[MapReduceUtil.DEST];
			String year = values[MapReduceUtil.YEAR];
			String month = values[MapReduceUtil.MONTH];
			String arrivalDelayStr = values[MapReduceUtil.ARR_DELAY];
			
			if (!mapReduceUtil.checkIfFileHeader(year)){
				return;
			}
			
			//Extract the values from String
			int arrivalDel;
			try{
				arrivalDel = Integer.parseInt(arrivalDelayStr);
			} catch(NumberFormatException e){
				return;
			}

			String season = mapReduceUtil.getSeasonForMonth(month);
			//Calculating and setting the Destination Arrival Delay's Map reduce key value
			String mapperKeyStr = mapReduceUtil.mergeStringWithDelimiter(destination, season, year);
			mapperValue.set(arrivalDel > 0 ? arrivalDel : 0);			
			mapperKey.set(mapperKeyStr);
			output.collect(mapperKey, mapperValue);
		}
	}

	public static class DelayBySeasonReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable reduceValue = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			int delay = 0;
			while(values.hasNext()){
				delay += values.next().get();
			}
			reduceValue.set(delay);
			output.collect(key, reduceValue);
		}
		
	}

	@Override
	public JobConf configureMapReduceJob(){
		JobConf job = new JobConf(DelayBySeasonPerYear.class);
		job.setJobName("Delay By Season of Year");
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(DelayBySeasonMapper.class);
		job.setCombinerClass(DelayBySeasonReducer.class);
		job.setReducerClass(DelayBySeasonReducer.class);
		
		return job;
	}


}
