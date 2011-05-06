package edu.columbia.gAnalyzer.worker;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import edu.columbia.gAnalyzer.util.CombinationGenerator;

public class ClusteringCoeffWorker extends MRGWorker {
	
	
	public static class ALCLusteringCoeffMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
		    String line = value.toString();
		    
			
		}
	}
	
	public static class ALClusteringCoeffReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    
		}
	}

	
	public static class ALCLusteringCoeffMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
		    String line = value.toString();
		    
			
		}
	}
	
	public static class ALClusteringCoeffReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
		}
	}
	
/*********************************************************************************************************************************/
	
	public static class ELCLusteringCoeffMapper1 extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			LongWritable nodeID = new LongWritable(Long.parseLong(tokenizer.nextToken()));
			LongWritable neighbor = new LongWritable(Long.parseLong(tokenizer.nextToken()));
			
			context.write(nodeID,neighbor);
		}
	}
	
	public static class ELClusteringCoeffReducer1 extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
		
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			List<LongWritable> set = new LinkedList<LongWritable>();
			for(LongWritable value : values) {
				set.add(value);
			}
			
	        CombinationGenerator<LongWritable> cg = new CombinationGenerator<LongWritable>(set, 2);
	        for(List<LongWritable> combination : cg) {
	            String edge = new String(combination.get(0).get() + ":" + combination.get(1));
	            Text outEdge = new Text(edge);
	            context.write(outEdge, key);
	        }

		}
	}
	
	
	public static class ELCLusteringCoeffMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
		    String line = value.toString();
		    
			
		}
	}
	
	public static class ELClusteringCoeffReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    
		}
	}
	
	@Override
	public void doWork() {
		// TODO Auto-generated method stub
		
	}
}
