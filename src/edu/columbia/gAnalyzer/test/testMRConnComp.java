package edu.columbia.gAnalyzer.test;

import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;

import org.apache.hadoop.util.GenericOptionsParser;

import edu.columbia.gAnalyzer.graph.MRAdjacencyListGraph;
import edu.columbia.gAnalyzer.graph.MREdgeListGraph;
import edu.columbia.gAnalyzer.graph.MRGraph;
import edu.columbia.gAnalyzer.job.GJobController;
import edu.columbia.gAnalyzer.job.JobType;
/**
 * test connected components sort of graph in edge list format.
 * 
 * @author Vishal Srivastava (vs2370@columbia.edu)
 *
 */

public class testMRConnComp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: <program> <in> <out>");
	      System.exit(2);
	    }
	    
		MRGraph mrg = new MREdgeListGraph(otherArgs[0]);
		GJobController gjc = GJobController.getGJobController();
		try {
			Long jobID;
			JobType jtype = JobType.CONCOMP; // define the job type
			jobID = gjc.createJob(mrg, otherArgs[1], jtype);
			gjc.startJob(jobID);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalClassFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
