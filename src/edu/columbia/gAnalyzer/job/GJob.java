package edu.columbia.gAnalyzer.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.columbia.gAnalyzer.graph.MRGraph;
import edu.columbia.gAnalyzer.worker.ClusteringCoeffWorker;
import edu.columbia.gAnalyzer.worker.DegreeDistWorker;

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
		super();
		outputPath = "/tmp";
		conf = new Configuration();
	}
	
	public GJob(String outputDirectory) throws IOException {
		super();
		outputPath = outputDirectory;
		conf = new Configuration();
	}
	
	public GJob(MRGraph mg,JobType jtype, String outputDirectory) throws IOException {
		this.mrgraph = mg;
		this.jobtype = jtype;
		this.outputPath = outputDirectory;
		conf = new Configuration(); 
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
		
		setInputFormatClass(FileInputFormat.class);
		setMapperClass(ClusteringCoeffWorker.CLusteringCoeffMapper.class);
		setReducerClass(ClusteringCoeffWorker.ClusteringCoeffReducer.class);
		setOutputKeyClass(LongWritable.class);
		setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(this, new Path(mrgraph.getInputFilesPath()));
		FileOutputFormat.setOutputPath(this, new Path(outputPath));
		
		waitForCompletion(true); //submits the job, waits for it to be completed.

	}

	public void startDDWorker() throws IOException, ClassNotFoundException, InterruptedException {

		setInputFormatClass(FileInputFormat.class);
		setMapperClass(DegreeDistWorker.ALDegreeDistMapper.class);
		setReducerClass(DegreeDistWorker.ALDegreeDistReducer.class);
		setOutputKeyClass(LongWritable.class);
		setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(this, new Path(mrgraph.getInputFilesPath()));
		FileOutputFormat.setOutputPath(this, new Path(outputPath));
		
		waitForCompletion(true); //submits the job, waits for it to be completed.
	}
	
	public void startMSWorker() {
		
	}
	
	public void startCOMWorker() {
		
	}
}
