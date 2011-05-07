package edu.columbia.gAnalyzer.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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
	
	public static class ELCLusteringCoeffMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			LongWritable nodeID = new LongWritable(Long.parseLong(tokenizer.nextToken()));
			LongWritable neighbor = new LongWritable(Long.parseLong(tokenizer.nextToken()));
			Text outNodeID = new Text(nodeID.toString());
			context.write(outNodeID,neighbor);
			System.out.println("Key :" + outNodeID + " Value : " + neighbor);
		}
	}
	
	public static class ELClusteringCoeffReducer1 extends Reducer<Text, LongWritable, Text, Text> {
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			
			Iterator<LongWritable> itr = values.iterator();
			List<Text> neighbors = new ArrayList<Text>();
			
			while(itr.hasNext()){
				neighbors.add(new Text(itr.next().toString()));
//				System.out.println("Key :" + key + " Values :" + itr.next().toString());
			}
			
			// emit actual edges too
			Iterator<Text> iter = neighbors.iterator();
			while(iter.hasNext()) {
				context.write(new Text(key+":"+iter.next().toString()), key);
			}
			
//			
//			// no need to combinate since only 2 nodes present
			if(neighbors.size() == 2) {
				String edge = new String(neighbors.get(0) + ":" + neighbors.get(1));
				Text outEdge = new Text(edge);
				context.write(outEdge,key);
				//System.out.println("Key :" + outEdge + " Value : " + key);
			} else if(neighbors.size() < 2) {
				
			} else {

				CombinationGenerator<Text> cg = new CombinationGenerator<Text>(neighbors, 2);
				for(List<Text> combination : cg) {
					String edge = new String(combination.get(0) + ":" + combination.get(1));
					Text outEdge = new Text(edge);
					context.write(outEdge, key);
					//System.out.println("Key :" + outEdge + " Value : " + key);
				}
			}
			
			System.out.println("Key :" + key + " Values :" + neighbors.toString());
		}
	}
	
	
	public static class ELCLusteringCoeffMapper2 extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line);
		    String edge = tokenizer.nextToken();
		    String vertex = tokenizer.nextToken();
		    context.write(new Text(edge), new Text(vertex));
			
		}
	}
	
	public static class ELClusteringCoeffReducer2 extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    
				HashSet<Text> vertices = new HashSet<Text>();
				
				double triangleFlag = 0;
				String edge = key.toString();
				String [] tokens = edge.split(":");
				int edgeCount = 0;
				if(tokens.length == 3) {
					for(Text value : values) {
						edgeCount++;
						vertices.add(value);
					}
//					triangleFlag = (double)(vertices.size());
//					Double trFlag = new Double(triangleFlag);
//					context.write(new Text("countMe"), new Text(trFlag.toString()));
					if(edgeCount ==2) {
						context.write(new Text("countMe"), new Text("1"));
					}
				}
		}
	}
	
	@Override
	public void doWork() {
		// TODO Auto-generated method stub
		
	}
}
