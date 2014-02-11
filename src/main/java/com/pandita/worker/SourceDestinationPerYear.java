/**
 * 
 */
package com.pandita.worker;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class SourceDestinationPerYear implements IBaseMapReduce{
	public static class MyMapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text mapperKey = new Text();
		private IntWritable mapperValue = new IntWritable(1);
		private MapReduceUtil mapReduceUtil = new MapReduceUtil();

		@Override
		public void map(LongWritable arg0, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String[] parts = line.split(",");

			String originString = parts[MapReduceUtil.ORIGIN];
			String destinationString = parts[MapReduceUtil.DEST];
			String yearString = parts[MapReduceUtil.YEAR];
			
			if (!mapReduceUtil.checkIfFileHeader(yearString)){
				return;
			}
			
			String flightCombinationString = mapReduceUtil.mergeStringWithDelimiter(
							originString,
							destinationString,
							yearString);

			mapperKey.set(flightCombinationString);

			output.collect(mapperKey, mapperValue);
		}
	}

	public static class MyReduceClass extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		private static IntWritable flightCount = new IntWritable();

		@Override
		public void reduce(Text flightNumber, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += 1;
				values.next();
			}

			flightCount.set(sum);
			output.collect(flightNumber, flightCount);
		}
	}

	@Override
	public JobConf configureMapReduceJob(){
		JobConf job = new JobConf(SourceDestinationPerYear.class);
		job.setJobName("Source Destination Flight Count");

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyMapClass.class);
		job.setReducerClass(MyReduceClass.class);
		
		return job;
	}
}