/**
 * 
 */
package com.pandita.runner;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import com.pandita.worker.IBaseMapReduce;

/**
 * @author blackhat
 *
 */
public class BaseRunner {

	public static final String DELIMITER = ":";
	
	/**
	 * Responsible for running the job with the given configuration
	 * @param job
	 * @param outputPath
	 * @param inputPaths
	 * @throws IOException
	 */
	public void runJob(JobConf job, String outputPath, String... inputPaths) throws IOException{
		if (job == null){
			throw new IllegalArgumentException();
		}
		
		//Create an array of given input paths
		Path[] paths = new Path[inputPaths.length];
		for (int i=0; i<inputPaths.length; i++){
			paths[i] = new Path(inputPaths[i]);
		}
		
		FileInputFormat.setInputPaths(job, paths);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		JobClient.runJob(job);
	}
	
	/**
	 * Returns the {@code JobConf} instance for the input Map Reduce configured for that class  
	 * @param clazz
	 * @return
	 */
	public <T extends IBaseMapReduce> JobConf getJobForClass(Class<T> clazz){
		
		JobConf job = null;
			try {
				job = clazz.newInstance().configureMapReduceJob();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		return job;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		BaseRunner configUtil = new BaseRunner();
		
		Class clazz = Class.forName(args[0]);
		
		JobConf job = configUtil.getJobForClass(clazz);
		
		String[] inputPaths = new String[21];
		int year = 1988;
		for (int i = 0; i<=20; i++){
			inputPaths[i] = "s3://airlinedataset/"+year+".csv";
			++year;
		}
		
		configUtil.runJob(job, "s3://airlinedatasetmroutput/"+job.getJobName(), inputPaths);
	}
}
