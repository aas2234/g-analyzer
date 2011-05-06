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
 * In-degree distribution calculation for graph in edge list format.
 * 
 * @author Vishal Srivastava (vs2370@columbia.edu)
 *
 */

public class InDegreeWorker extends MRGWorker{
	/**
	 * Mapper for Indegree distribution calculation for graph in edge list format.
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
 * Reducer for Indegree distribution calculation for graph in edge list format.
 * 
 * @author Vishal Srivastava (vs2370@columbia.edu)
 *
 */
public static class ELINDegreeDistReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
	
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    
		int indegree = 0;
		for(IntWritable value : values) {
			indegree = indegree + value.get();
		}
		context.write(key, new IntWritable(indegree));
	}
}
	@Override
	public void doWork() {
		// TODO Auto-generated method stub
		
	}

}
