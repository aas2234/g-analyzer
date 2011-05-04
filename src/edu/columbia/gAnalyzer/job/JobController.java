package edu.columbia.gAnalyzer.job;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;


/**
 * The JobController class is an abstract factory for creating new jobs for Hadoop.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public abstract class JobController {

	public abstract Long createJob() throws IOException;
	public abstract Job getJob(Long jobID);
	public abstract void stopJob(Long jobID);
	
}
