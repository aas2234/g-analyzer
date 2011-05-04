package edu.columbia.gAnalyzer.job;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

import edu.columbia.gAnalyzer.graph.MRGraph;


enum JobType {CLUSTERING_COEFF, DEGREE_DIST, MOTIF_STATS, COMMUNITIES}; 

/**
 * The GJob class extends Hadoop's Job class for hadoop jobs on graphs.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public class GJob extends Job implements Runnable{

	private MRGraph mrgraph;
	private JobType jobtype;
	
	public GJob() throws IOException {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public GJob(MRGraph mg,JobType jtype) throws IOException {
		this.mrgraph = mg;
		this.jobtype = jtype;
	}
	
	
	public void start() {
		this.run();
	}
	
	@Override
	public void run() {
		// set the job configurations, mapper, reducer and start the job
		
	}

}
