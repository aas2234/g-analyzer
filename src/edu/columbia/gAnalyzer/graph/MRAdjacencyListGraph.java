package edu.columbia.gAnalyzer.graph;

import java.util.List;
import java.util.Map;

/**
 * The MRAdjacencyListGraph class must be used for graphs that are represented in the adjacency list format.
 * It implements the methods for computing statistics on the graph.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public class MRAdjacencyListGraph extends MRGraph{

	public MRAdjacencyListGraph() {}
	
	public MRAdjacencyListGraph(String inputFilesPath) {
		super(inputFilesPath);
	}

	@Override
	public double getClusteringCoefficient(long M, long R) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<Long, Integer> getDegreeDistribution(long M, long R) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Long, Long> getMotifStatistics(long M, long R, int motifSize) {
		// TODO Auto-generated method stub
		return null;
	}
}
