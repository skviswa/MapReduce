package preprocess;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import mapping.ConvMatrixMapper;
import prank.MatrixMapper;
import prank.MatrixReducer;
import prank.MatrixRowMapper;
import preprocess.FirstMapper.ProcessMapper;
import topk.TopKMapper;
import topk.TopKReducer;
import transpose.TransposeMapper;
import transpose.TransposeReducer;

// This class is the base class to run all jobs
public class Preprocess { 

	  public static class MatrixPartitioner  // To partition a station id to each reducer
	    extends Partitioner<Text, Text> {

	    @Override
	    public int getPartition(Text key, Text value, int numPartitions) {
	      // multiply by  to perform some mixing
	      String index = key.toString();
	      //return ((Math.abs(index.hashCode()) & Integer.MAX_VALUE) % numPartitions);
	      return Math.abs(Integer.parseInt(index)) % numPartitions;  // drop the year and hash only station id
	    }
	  }

	public static enum MoreIterations {
        numVertices,		// Flag for counting vertices in the graph
        deltaCounter,		// Flag for keeping track of probabilities of dangling nodes
        mapping,	
        stage,				// Flag to keep track of when to initialize page ranks with 1/V
        nondangling
    };
	public static void main(String[] args) throws Exception{
		long startime, endtime, startime1, endtime1;
		float timetaken;
		
		int code = 0;									// For keeping track of exit condition
		String input,output,input1="";							// For creating input, output paths for iterative jobs
	    Configuration conf = new Configuration();
	    conf.set("mfile", args[1]);  									// Need to know relative path when using MultiOutput Format
	    
	    startime = System.currentTimeMillis();
	    
	    Job job = Job.getInstance(conf, "job");
	    job.setJarByClass(Preprocess.class);
	    job.setMapperClass(ProcessMapper.class);	// This takes care of pre processing stage
	    job.setReducerClass(FirstReducer.class);	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);					// Send all the results to single Reduce task
	    MultipleOutputs.addNamedOutput(job, "URL", TextOutputFormat.class, Text.class, Text.class); // Write the Mapping (pageName -> Number) to a separate path
	    MultipleOutputs.addNamedOutput(job, "D", TextOutputFormat.class, Text.class, Text.class);   // Write the list of dangling nodes to a separate path
	    MultipleOutputs.addNamedOutput(job, "Mapping", TextOutputFormat.class, Text.class, Text.class);    // Write the Mapping: (Number -> pageName) to a separate path  
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    code = job.waitForCompletion(true) ? 0 : 1;

	    conf.set("matmap", new Path(args[1] + "MatMap/").toString());  // This path is used to load the Mapping: (pageName -> Number) in the job
	    conf.set("D", new Path(args[1] + "D/").toString());            // This path is used to load the list of Dangling nodes in the job
	    conf.set("mapping", new Path(args[1] + "Mapping/").toString());  // This path is used to load the Mapping: (Number -> pageName)
   
	    long niter = 1; 	// Starting the first of 10 iterations

    	long nV = job.getCounters().findCounter(MoreIterations.numVertices).getValue();        // Get the count of number of pageNames in the dataset
    	long ndang = job.getCounters().findCounter(MoreIterations.nondangling).getValue();    // Get a count of the non-dangling nodes in this list
    	
	    conf.setLong("numVertices", nV);          // Counting No. of Vertices in pre-process stage. Passing the value to conf
	    conf.setLong("dangling", (nV - ndang));
    	conf.setLong("stage", 0);	              // This stage tracks whether to initialize pageranks to 1/V or compute them

    	// This job will map the URLs in the adjacency list to the respective numbers
    	//  This is a map only job
    	Job jobmap = Job.getInstance(conf, "jobmap");   
	    												
    	String opath = args[1]+"0";
	    jobmap.setJarByClass(Preprocess.class);
	    jobmap.setMapperClass(ConvMatrixMapper.class);	
	    jobmap.setReducerClass(FirstReducer.class);	
	    jobmap.setMapOutputKeyClass(Text.class);
	    jobmap.setMapOutputValueClass(Text.class);	    
	    jobmap.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(jobmap, new Path(args[1]));
	    FileOutputFormat.setOutputPath(jobmap, new Path(opath));
	    code = jobmap.waitForCompletion(true) ? 0 : 1;

	    input = args[1]+"0";
	    output = args[1]+"tr";
	    
	    // This job converts the number mapped result in to a row major form matrix representation
	    // The partitioner reads the row number and assigns it to one of the reducer machines
	    Job jobtr = Job.getInstance(conf, "jobtr");                   
	    jobtr.setJarByClass(Preprocess.class);
     	jobtr.setMapperClass(TransposeMapper.class);
     	jobtr.setReducerClass(TransposeReducer.class);
	    jobtr.setOutputKeyClass(Text.class);
	    jobtr.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(jobtr, new Path(input));
	    FileOutputFormat.setOutputPath(jobtr, new Path(output));
	    code = jobtr.waitForCompletion(true) ? 0 : 1;
	    
	    endtime = System.currentTimeMillis();
	    
	    timetaken = (float) (endtime - startime)/60;
	    
	    // My Step 1 tracks time from parsing input to producing a matrix representation
	    System.err.println("STEP 1: "+Float.toString(timetaken));
	    

	    input = args[1]+"tr";	// Update the path with the iteration
	    output = args[1]+niter; // Creating new output folders to hold logs of each iteration
	    
	    // Job for 10 iterations of PageRank, along with initializing the ranks to 1/V 
	    // This is a map only job

	    
	    startime = System.currentTimeMillis();
	    while(niter < 11) {
	    startime1 = System.currentTimeMillis();	
	    Job job1 = Job.getInstance(conf, "job1");
	    job1.setJarByClass(Preprocess.class);
     	job1.setMapperClass(MatrixRowMapper.class);
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(Text.class);	    
	    job1.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job1, new Path(input));
	    FileOutputFormat.setOutputPath(job1, new Path(output));
	    code = job1.waitForCompletion(true) ? 0 : 1;
	    endtime1 = System.currentTimeMillis();
	    timetaken = (float) (endtime1 - startime1)/60;
	    // STEP 2 tracks 
	    if(niter == 1)
	    	System.err.println("STEP 2: "+Float.toString(timetaken));
	    
	    niter++;
	    input1 = args[1]+(niter-1);	// Update the path with the iteration
	    output = args[1]+niter;
	    if(niter == 2) {
	    	conf.setLong("stage", 1);	// This is for indicating initialization with 1/V is done at first stage
		    conf.set("output", new Path(args[1] + "1/").toString());       // This path is used to load the PageRank result in the job

		    }										
    	
	   }    

    	
/*		// THIS JOB RUNS THE COLUMN PARTITION VERSION OF THE ALGORITHM
 * 		// FOR 10 ITERATIONS TO COMPUTE THE PAGE RANK
	    input = args[1]+"0";
	    output = args[1]+niter; // Creating new output folders to hold logs of each iteration
	    // Job for 10 iterations of PageRank
	    startime = System.currentTimeMillis();
	    while(niter < 11) {
	    startime1 = System.currentTimeMillis();	
	    Job job1 = Job.getInstance(conf, "job1");
	    job1.setJarByClass(Preprocess.class);
     	job1.setMapperClass(MatrixMapper.class);
     	job1.setPartitionerClass(MatrixPartitioner.class);
	    job1.setReducerClass(MatrixReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job1, new Path(input));
	    FileOutputFormat.setOutputPath(job1, new Path(output));
	    code = job1.waitForCompletion(true) ? 0 : 1;
	    endtime1 = System.currentTimeMillis();
	    timetaken = (float) (endtime1 - startime1)/60;
	    if(niter == 1)
	    	System.err.println("STEP 2: "+Float.toString(timetaken));
	    niter++;
	    input1 = args[1]+(niter-1);	// Update the path with the iteration
	    output = args[1]+niter;
	    if(niter == 2) {
	    	conf.setLong("stage", 1);	// This is for indicating initialization with 1/V is done at first stagef
		    }										// next mapper iteration, save the value in a new variable
    	
	   }
*/	
	    endtime = System.currentTimeMillis();
	    timetaken = (float) (endtime - startime)/60;
	    
	    // Here is the end of step 3, which is completion of pagerank for 10 iterations
	    
	    System.err.println("STEP 3: "+Float.toString(timetaken)); 
	    
	    startime = System.currentTimeMillis();
	    
	    // Last Job for Top100 Records
	    
	    Job job2 = Job.getInstance(conf, "job2");
	    job2.setJarByClass(Preprocess.class);
	    job2.setMapperClass(TopKMapper.class);
	    job2.setReducerClass(TopKReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setMapOutputKeyClass(NullWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job2, new Path(input1));
	    FileOutputFormat.setOutputPath(job2, new Path(output));
	    code = job2.waitForCompletion(true) ? 0 : 1;
	    endtime = System.currentTimeMillis();
	    timetaken = (float) (endtime - startime)/60;
	    
	    // Final step of computing  Top 100 records
	    System.err.println("STEP 4: "+Float.toString(timetaken));

	    // return code;	
		System.exit(code);
	}
}
