/**
 * 
 */
package com.pandita.worker;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
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
public class AverageMonthlyDepartureDelay implements IBaseMapReduce{
	
	public static class MyMapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, FloatWritable> {

		private Text mapperKey = new Text();
		private FloatWritable mapperValue = new FloatWritable();
		private MapReduceUtil mapReduceUtil = new MapReduceUtil();

		@Override
		public void map(LongWritable arg0, Text value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String[] parts = line.split(",");
			
			String monthString = parts[MapReduceUtil.MONTH];
			String yearString = parts[MapReduceUtil.YEAR];
			String mapperKeyStr = mapReduceUtil.mergeStringWithDelimiter(monthString, yearString);
			
			if (!mapReduceUtil.checkIfFileHeader(yearString)){
				return;
			}
			
			mapperKey.set(mapperKeyStr);

			// parts[15] is the departure delay in minutes
			String  depDelayInMinsStr = parts[MapReduceUtil.DEP_DELAY];
			
			int depDelayInMin = 0;
			try{
				depDelayInMin = Integer.parseInt(depDelayInMinsStr);
			}
			catch(NumberFormatException e){
				return;
			}
			
			mapperValue.set(depDelayInMin);

			output.collect(mapperKey, mapperValue);

		}
	}

	public static class MyReduceClass extends MapReduceBase implements
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		private FloatWritable sumDepDelayPerMonth = new FloatWritable();

		@Override
		public void reduce(Text monthNumber, Iterator<FloatWritable> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {
			float sum = 0;
			float counter = 0;
			while (values.hasNext()) {
				counter++;
				sum += values.next().get();
			}

			sumDepDelayPerMonth.set(sum / counter);
			output.collect(monthNumber, sumDepDelayPerMonth);
		}

	}

	@Override
	public JobConf configureMapReduceJob(){
		JobConf job = new JobConf(AverageMonthlyDepartureDelay.class);
		job.setJobName("Average Monthly Departure Delay");

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(MyMapClass.class);
		job.setReducerClass(MyReduceClass.class);

		return job;
	}

}
