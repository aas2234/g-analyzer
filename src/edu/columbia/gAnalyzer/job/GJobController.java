package edu.columbia.gAnalyzer.job;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Job;


/**
 * The GJobController is a concrete factory class for creating and destroying Hadoop jobs.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public class GJobController extends JobController {

	private HashMap<Long, GJob> jobList;
	private Long lastIndex;
	private static boolean created;
	private static GJobController gjobFactory;
	
	public static GJobController getGJobController() {
		if(!created) {
			gjobFactory = new GJobController();
			created = true;
		} 
		
		return gjobFactory; 
	}
	
	@Override
	public Long createJob(MRGraph mrg) throws IOException{
		lastIndex = lastIndex + 1;
		GJob job = new GJob();
		jobList.put(new Long(lastIndex), job);
		
		return lastIndex;
	}

	@Override
	public Job getJob(Long jobID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stopJob(Long jobID) {
		// TODO Auto-generated method stub

	}

}
