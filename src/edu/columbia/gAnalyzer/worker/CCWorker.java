package edu.columbia.gAnalyzer.worker;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import edu.columbia.gAnalyzer.util.DisjointSet;;
/**
 * Connected Component of graph in edge list format.
 * 
 * @author Vishal Srivastava (vs2370@columbia.edu)
 *  By reference to Aditya Deo (adityadeo@gmail.com)
 *
 */


public class CCWorker extends MRGWorker{
	  public static class CC_Mapper1 extends Mapper<LongWritable, Text, IntWritable, Text>
	    {

	        @Override
	        public void map (LongWritable key, Text value, final Context context) throws IOException, InterruptedException
	        {
	        	int random_no = 20;
	        	Random generator = new Random();
	        	int r = generator.nextInt(random_no) + 1;
	        	context.write(new IntWritable(r) , value);
	        }
	    }

	    public static class CC_Reducer1 extends Reducer<IntWritable, Text, IntWritable, Text>
	    {
	    	private final String delim = "\t";
	    	private static DisjointSet dis_joint_set = new DisjointSet();
	        @Override
	        public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	        {
	            
	            int u=0, v=0;
	            for (Text value : values)
	            {
	            	String line = value.toString();
	                StringTokenizer tokenizer = new StringTokenizer(line);
	                if (tokenizer.hasMoreTokens())
	                {
	                	u = Integer.parseInt(tokenizer.nextToken(delim)); 
	                	v = Integer.parseInt(tokenizer.nextToken(delim)); 
	                }
	                
	                if(dis_joint_set.findSet(u) == null)
	                {
	                	dis_joint_set.makeSet(u);
	                }
	                if(dis_joint_set.findSet(v) == null)
	                {
	                	dis_joint_set.makeSet(v);
	                }
	                if(dis_joint_set.findSet(u) != dis_joint_set.findSet(v))
	                {
	                	dis_joint_set.union(u, v);
	                	context.write(key, value);
	                }
	            }
	        }
	    }
	    
	    public static class CC_Mapper2 extends Mapper<LongWritable, Text, Text, Text>
	    {

	    	private Text SpecialSymbol = new Text("$");
	    	private final String delim = "\t";

	        @Override
	        public void map (LongWritable key, Text value, final Context context) throws IOException, InterruptedException
	        {
	        	String line = value.toString();
	        	line = line.split(delim, 2)[1];
	        	context.write(SpecialSymbol ,  new Text(line));
	        }
	    }
	    
	    
	    public static class CC_Reducer2 extends Reducer<Text, Text, Text, IntWritable>
	    {
	    	private final String delim = "\t";
	    	private static DisjointSet dis_joint_set = new DisjointSet();
	    	
	    	
	        @Override
	        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	        {
	            int u = 0, v = 0;
	            for (Text value : values)
	            {
	            	String line = value.toString();
	            	StringTokenizer tokenizer = new StringTokenizer(line);
	                if (tokenizer.hasMoreTokens())
	                {
	                	u = Integer.parseInt(tokenizer.nextToken(delim)); 
	                	v = Integer.parseInt(tokenizer.nextToken(delim)); 
	                }
	                
	                if(dis_joint_set.findSet(u) == null)
	                {
	                	dis_joint_set.makeSet(u);
	                }
	                if(dis_joint_set.findSet(v) == null)
	                {
	                	dis_joint_set.makeSet(v);
	                }
	                if(dis_joint_set.findSet(u) != dis_joint_set.findSet(v))
	                {
	                	dis_joint_set.union(u, v);
	                }
	            }
	        	context.write(key, new IntWritable(dis_joint_set.get_number_connected_components()));
	        }
	    }


		@Override
		public void doWork() {
			// TODO Auto-generated method stub
			
		}
	}

