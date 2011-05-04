import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;

import edu.columbia.gAnalyzer.graph.MRAdjacencyListGraph;
import edu.columbia.gAnalyzer.graph.MRGraph;
import edu.columbia.gAnalyzer.job.GJobController;
import edu.columbia.gAnalyzer.job.JobType;

public class testMRGraph {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String outputPath = new String("/home/abhi/test/MRGraph/DD/op/");
		
		MRGraph mrg = new MRAdjacencyListGraph();
		GJobController gjc = GJobController.getGJobController();
		try {
			Long jobID;
			JobType jtype = JobType.DEGREE_DIST; // define the job type
			jobID = gjc.createJob(mrg, outputPath, jtype);
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
