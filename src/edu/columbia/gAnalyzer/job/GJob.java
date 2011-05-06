package edu.columbia.gAnalyzer.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.columbia.gAnalyzer.graph.MRAdjacencyListGraph;
import edu.columbia.gAnalyzer.graph.MREdgeListGraph;
import edu.columbia.gAnalyzer.graph.MRGraph;
import edu.columbia.gAnalyzer.worker.ClusteringCoeffWorker;
import edu.columbia.gAnalyzer.worker.DegreeDistWorker;
import edu.columbia.gAnalyzer.worker.InDegreeWorker;

/**
 * The GJob class extends Hadoop's Job class for hadoop jobs on graphs.
 * 
 * @author Abhishek Srivastava (aas2234@columbia.edu)
 *
 */
public class GJob extends Job implements Runnable{

	private MRGraph mrgraph;
	private JobType jobtype;
	private String outputPath;
	private Configuration conf;
	
	public GJob() throws IOException {
		super(new Configuration());
		outputPath = "/tmp";
		/*conf = new Configuration();*/
	}
	
	public GJob(String outputDirectory) throws IOException {
		super(new Configuration());
		outputPath = outputDirectory;
		/*conf = new Configuration();*/
	}
	
	public GJob(MRGraph mg,JobType jtype, String outputDirectory) throws IOException {
		super(new Configuration());
		this.mrgraph = mg;
		this.jobtype = jtype;
		this.outputPath = outputDirectory;
		/*conf = new Configuration();*/ 
	}
	
	public void start() {
		this.run();
	}
	
	@Override
	public void run() {
		switch(jobtype) {
		case CLUSTERING_COEFF:
			
			try {
				startCCWorker();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case DEGREE_DIST:
			try {
				startDDWorker();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case INDEGREE:
			try {
				startIndegreeWorker();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		case MOTIF_STATS:
			startMSWorker();
			break;
		case COMMUNITIES:
			startCOMWorker();
	
			break;
		default:
			;
		}
		
	}
	
	public void startCCWorker() throws IOException, ClassNotFoundException, InterruptedException {

		setJobName(JobType.CLUSTERING_COEFF.toString());
		setJarByClass(ClusteringCoeffWorker.class);
		//TODO: Fix after figuring out exact algorithm
		
		setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(LongWritable.class);
		
        setInputFormatClass(TextInputFormat.class);
        setOutputFormatClass(TextOutputFormat.class);
   
        setOutputKeyClass(Text.class);
        setOutputValueClass(LongWritable.class);
   
		FileInputFormat.addInputPath(this, new Path(mrgraph.getInputFilesPath()));
		FileOutputFormat.setOutputPath(this, new Path(outputPath));

		// handle both forms of graphs as input 
		
		if(mrgraph instanceof MRAdjacencyListGraph) {
			setMapperClass(ClusteringCoeffWorker.ALCLusteringCoeffMapper1.class);
			//setCombinerClass(ClusteringCoeffWorker.ALClusteringCoeffReducer1.class);
			setReducerClass(ClusteringCoeffWorker.ALClusteringCoeffReducer1.class);
		} else if (mrgraph instanceof MREdgeListGraph) {
			setMapperClass(ClusteringCoeffWorker.ELCLusteringCoeffMapper1.class);
			//setCombinerClass(ClusteringCoeffWorker.ELClusteringCoeffReducer1.class);
			setReducerClass(ClusteringCoeffWorker.ELClusteringCoeffReducer1.class);
		}

		waitForCompletion(true); //submits the job, waits for it to be completed.

		/********* SECOND TASK ***********/
		
		setMapOutputKeyClass(Text.class);
        setMapOutputValueClass(LongWritable.class);
		
        setInputFormatClass(TextInputFormat.class);
        setOutputFormatClass(TextOutputFormat.class);
   
        setOutputKeyClass(Text.class);
        setOutputValueClass(LongWritable.class);
   
		FileInputFormat.addInputPath(this, new Path(mrgraph.getInputFilesPath()));
		FileOutputFormat.setOutputPath(this, new Path(outputPath));

		// handle both forms of graphs as input 
		
		if(mrgraph instanceof MRAdjacencyListGraph) {
			setMapperClass(ClusteringCoeffWorker.ALCLusteringCoeffMapper2.class);
			//setCombinerClass(ClusteringCoeffWorker.ALClusteringCoeffReducer1.class);
			setReducerClass(ClusteringCoeffWorker.ALClusteringCoeffReducer2.class);
		} else if (mrgraph instanceof MREdgeListGraph) {
			setMapperClass(ClusteringCoeffWorker.ELCLusteringCoeffMapper2.class);
			//setCombinerClass(ClusteringCoeffWorker.ELClusteringCoeffReducer1.class);
			setReducerClass(ClusteringCoeffWorker.ELClusteringCoeffReducer2.class);
		}

		waitForCompletion(true); //submits the job, waits for it to be completed.
	
	}

	public void startDDWorker() throws IOException, ClassNotFoundException, InterruptedException {

		setJobName(JobType.DEGREE_DIST.toString());
		setJarByClass(DegreeDistWorker.class);
		setOutputKeyClass(LongWritable.class);
		setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(this, new Path(mrgraph.getInputFilesPath()));
		FileOutputFormat.setOutputPath(this, new Path(outputPath));

		// handle both forms of graphs as input 
		
		if(mrgraph instanceof MRAdjacencyListGraph) {
			setMapperClass(DegreeDistWorker.ALDegreeDistMapper.class);
			setCombinerClass(DegreeDistWorker.ELDegreeDistReducer.class);
			setReducerClass(DegreeDistWorker.ALDegreeDistReducer.class);
		} else if (mrgraph instanceof MREdgeListGraph) {
			setMapperClass(DegreeDistWorker.ELDegreeDistMapper.class);
			setCombinerClass(DegreeDistWorker.ELDegreeDistReducer.class);
			setReducerClass(DegreeDistWorker.ELDegreeDistReducer.class);
		}

		waitForCompletion(true); //submits the job, waits for it to be completed.

	}
	
	public void startIndegreeWorker() throws IOException, ClassNotFoundException, InterruptedException {

		setJobName(JobType.INDEGREE.toString());
		setJarByClass(InDegreeWorker.class);
		setOutputKeyClass(LongWritable.class);
		setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(this, new Path(mrgraph.getInputFilesPath()));
		FileOutputFormat.setOutputPath(this, new Path(outputPath));

		// handle both forms of graphs as input 
		
		if(mrgraph instanceof MRAdjacencyListGraph) {
			/*setMapperClass(DegreeDistWorker.ALDegreeDistMapper.class);
			setReducerClass(DegreeDistWorker.ALDegreeDistReducer.class);*/
		} else if (mrgraph instanceof MREdgeListGraph) {
			setMapperClass(InDegreeWorker.ELINDegreeDistMapper.class);
			setCombinerClass(InDegreeWorker.ELINDegreeDistReducer.class);
			setReducerClass(InDegreeWorker.ELINDegreeDistReducer.class);
			
		}

		waitForCompletion(true); //submits the job, waits for it to be completed.
	}
	public void startMSWorker() {
		
	}
	
	public void startCOMWorker() {
		
	}
}
