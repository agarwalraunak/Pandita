/**
 * 
 */
package com.pandita.worker;

import org.apache.hadoop.mapred.JobConf;

/**
 * @author blackhat
 *
 */
public interface IBaseMapReduce {

	
	/**
	 * Returns the {@code JobConf} Configured Map Reduce Job for the Task  
	 * @return {@code JobConf}
	 */
	public JobConf configureMapReduceJob();
}
