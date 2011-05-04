package edu.columbia.gAnalyzer.job;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

import edu.columbia.gAnalyzer.graph.MRGraph;


/**
 * The GJob class extends Hadoop's Job class for hadoop jobs on graphs.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public class GJob extends Job implements Runnable{

	
	MRGraph mrgraph;
	
	public GJob() throws IOException {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public GJob(MRGraph mg) throws IOException {
		this.mrgraph = mg;
	}
	
	public void start() {
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
