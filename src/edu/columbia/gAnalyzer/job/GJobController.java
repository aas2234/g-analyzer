package edu.columbia.gAnalyzer.job;

import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;
import java.util.HashMap;

import edu.columbia.gAnalyzer.graph.MRGraph;


/**
 * The GJobController is a concrete factory class for creating and destroying Hadoop jobs.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 * 
 * //TODO : stopped jobs should return their IDs back to the factory 
 */
public class GJobController extends JobController {

	private static HashMap<Long, GJob> jobList;
	private long lastIndex;
	private static boolean created;
	private static GJobController gjobFactory;
	
	public static GJobController getGJobController() {
		if(!created) {
			gjobFactory = new GJobController();
			jobList = new HashMap<Long,GJob>();
			created = true;
		} 
		
		return gjobFactory; 
	}
	
	@Override
	public GJob getJob(Long jobID) {
		
		if(jobList.containsKey(jobID)) {
			return jobList.get(jobID);
		} else {
			return null;
		}
	}
	
	//TODO: Remove jobtype from here. Ugly and not conforming to parent's interface.
	//TODO: Consider the type of MRGraph to differentiate between hadnling edge-list and adj-list
	@Override
	public Long createJob(Object mrgraph, String outputDirectory, JobType jobtype) throws IOException, IllegalClassFormatException {
		
		if(mrgraph != null) {
			if(mrgraph instanceof MRGraph) {	

				MRGraph mrg = (MRGraph) mrgraph;
				lastIndex = lastIndex + 1;
				GJob job = new GJob(mrg,jobtype,outputDirectory);
				jobList.put(new Long(lastIndex), job);
				return lastIndex;
			} else {
				throw new IllegalClassFormatException("Object is not of type MRGraph");
			}
		} else {
			throw new NullPointerException("MRGraph object was null");
		}
	}
	
	@Override
	public void startJob(Long jobID) {
		GJob startJob = jobList.get(jobID);
		startJob.start();
	}
	
	@Override
	public void stopJob(Long jobID) throws IOException {
		GJob stopJob = jobList.get(jobID);
		stopJob.killJob();
	}

}
