package edu.columbia.gAnalyzer.util;

import edu.columbia.gAnalyzer.graph.MRGraph;

/**
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public interface GraphNormalizer {

	/**
	 * converts a graph from edge list to adjacency list format. This is essentially an IO intensive 
	 * unix filter to generate new files in the adjacency list format and reset MRGraph's fileList.
	 * 
	 * @param mrg MRGraph to convert
	 */
	public void convertToAdjacencyList(MRGraph mrg);
}
