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

public class DegreeDistWorker extends MRGWorker {

	
	
	/**
	 * Mapper for degree distribution calculation for graph in adjacency list format.
	 * 
	 * @author Abhishek Srivastava (aas2234@columbia.edu)
	 *
	 */
	public static class ALDegreeDistMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			int edgeSum = 0;
			LongWritable nodeID = new LongWritable(Long.parseLong(tokenizer.nextToken()));
			
			while (tokenizer.hasMoreTokens()) {
				edgeSum += 1;
				tokenizer.nextToken();
			}
			IntWritable edgeSUM = new IntWritable(edgeSum);
			context.write(nodeID,edgeSUM);
			
		}
	}
	

	/**
	 * Reducer for degree distribution calculation for graph in adjacency list format.
	 * 
	 * @author Abhishek Srivastava (aas2234@columbia.edu)
	 *
	 */
	public static class ALDegreeDistReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
	    
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}


	/**
	 * Mapper for degree distribution calculation for graph in edge list format.
	 * 
	 * @author Abhishek Srivastava (aas2234@columbia.edu)
	 *
	 */
	public static class ELDegreeDistMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		
		private IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			LongWritable nodeID = new LongWritable(Long.parseLong(tokenizer.nextToken()));
			
			context.write(nodeID,one);
			
		}
	}
	

	/**
	 * Reducer for degree distribution calculation for graph in edge list format.
	 * 
	 * @author Abhishek Srivastava (aas2234@columbia.edu)
	 *
	 */
	public static class ELDegreeDistReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    
			int sum = 0;
			for(IntWritable value : values) {
				sum = sum + value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	@Override
	public void doWork() {
		
		
	}
}
