package edu.columbia.gAnalyzer.worker;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Mapper for degree distribution calculation for graph in adjacency list format.
 * 
 * @author Vishal Srivastava (vs2370@columbia.edu)
 *
 */

public class InDegreeWorker extends MRGWorker{
	/**
	 * Mapper for degree distribution calculation for graph in edge list format.
	 * 
	 * @author Vishal Srivastava (vs2370@columbia.edu)
	 *
	 */
	
public static class ELINDegreeDistMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		
		private IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			tokenizer.nextToken();
			LongWritable nodeID = new LongWritable(Long.parseLong(tokenizer.nextToken()));
			context.write(nodeID,one);
			
		}
	}
	
/**
 * Reducer for degree distribution calculation for graph in edge list format.
 * 
 * @author Vishal Srivastava (vs2370@columbia.edu)
 *
 */
	public static class ELINDegreeDistReducer extends Reducer<LongWritable, IntWritable,Text , IntWritable> {
		
		public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
	    
			int indegree=0;
		
			while (values.hasNext()) {
				indegree += values.next().get();
			}
			output.collect(key, new IntWritable(indegree));
		}
	}
	@Override
	public void doWork() {
		// TODO Auto-generated method stub
		
	}

}
