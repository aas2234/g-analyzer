package edu.columbia.gAnalyzer.graph;

import java.util.List;
import java.util.Map;

/**
 * The MRGraph class represents the graph to be analyzed using the MapReduce framework.
 * It contains helper methods for retrieving statistics about the graph. The graph is 
 * not stored in memory but is read in as and when the mappers and reducers will process it.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public abstract class MRGraph {

	private String inputFilesPath;
	
	public MRGraph() {}
	
	/**
	 * constructor specifying files of graph
	 * @param fileList
	 */
	public MRGraph(String inputFilePath) {
		setFileList(inputFilePath);
	}
	
	
	/**
	 * sets the list of files to read in the graph from.
	 * The graph must be in an adjacency list format with nodes
	 * labelled as numeric (long) IDs.
	 * 
	 * @param fileList a list of Strings containing paths to the files
	 */
	public void setFileList(String inputFilesPath) {
		this.inputFilesPath = inputFilesPath;
	}
	
	/**
	 * gets the list of files the graph was read from.
	 * 
	 * @return list of paths to the files of the graph
	 */
	public String getInputFilesPath() {
		return inputFilesPath;
	}
	
	/**
	 * returns the degree distribution of the graph as a Map<NodeID, degree>
	 * 
	 * @param M number of mappers to use
	 * @param R number of reducers to use
	 * @return degree distribution of graph
	 */
	public abstract Map<Long, Integer> getDegreeDistribution(long M, long R);
	
	/**
	 * returns the clustering coefficient of the graph
	 * 
	 * @param M number of mappers to use
	 * @param R number of reducers to use
	 * @return clustering coefficient of graph
	 */
	public abstract double getClusteringCoefficient(long M, long R);
	
	
	/**
	 * returns count statistics of various motif IDs of size @param motifSize
	 * 
	 * @param M number of mappers to use
	 * @param R number of reducers to use
	 * @param motifSize motif size to compute statistics for
	 * @return map of motif ID and counts
	 */
	public abstract Map<Long, Long> getMotifStatistics(long M, long R, int motifSize);
}
