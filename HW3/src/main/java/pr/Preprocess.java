package pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pr.FirstMapper.ProcessMapper;
import prank.PageRankMapper;
import prank.PageRankReducer;
import topk.TopKMapper;
import topk.TopKReducer;

import org.apache.hadoop.mapreduce.Counters;

// This class is the base class to run all jobs
public class Preprocess { 

	public static enum MoreIterations {
        numVertices,		// Flag for counting vertices in the graph
        deltaCounter,		// Flag for keeping track of probabilities of dangling nodes
        olddeltaCounter,	// Flag to save value of delta counter to update result in next iteration
        stage,				// Flag to keep track of when to initialize page ranks with 1/V
        convergestatus     // To find if the results are converging
    };
	
	public static void main(String[] args) throws Exception{
		
		int code = 0;									// For keeping track of exit condition
		String input,output;							// For creating input, output paths for iterative jobs
	    Configuration conf = new Configuration();
	    
	    Job job = Job.getInstance(conf, "job");
	    job.setJarByClass(Preprocess.class);
	    job.setMapperClass(ProcessMapper.class);	// This takes care of pre processing stage
	    job.setReducerClass(FirstReducer.class);	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    code = job.waitForCompletion(true) ? 0 : 1;

	    long niter = 1; 	// Starting the first of 10 iterations

    	long nV = job.getCounters().findCounter(MoreIterations.numVertices).getValue();
	    conf.setLong("numVertices", nV); // Counting No. of Vertices in pre-process stage. Passing the value to conf
	    conf.setLong("deltaCounter", 0);
     	conf.setLong("olddeltaCounter", 0);
    	conf.setLong("stage", 0);	    
	    input = args[1];
	    output = args[1]+"-"+niter; // Creating new output folders to hold logs of each iteration

	    // Job for 10 iterations of PageRank
	    while(niter < 12) {
	    Job job1 = Job.getInstance(conf, "job1");
	    job1.setJarByClass(Preprocess.class);
    	job1.setMapperClass(PageRankMapper.class);
	    job1.setReducerClass(PageRankReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job1, new Path(input));
	    FileOutputFormat.setOutputPath(job1, new Path(output));
	    code = job1.waitForCompletion(true) ? 0 : 1;
	    niter++;
	    input = args[1]+"-"+(niter-1);	// Update the path with the iteration
	    output = args[1]+"-"+niter;
    	long dcnt = job1.getCounters().findCounter(MoreIterations.deltaCounter).getValue();
	    conf.setLong("deltaCounter", 0);	// A fresh iteration starts with a deltacounter reset to 0
    	job1.getCounters().findCounter(MoreIterations.deltaCounter).setValue(0);
    	job1.getCounters().findCounter(MoreIterations.olddeltaCounter).setValue(dcnt);
    	conf.setLong("olddeltaCounter", dcnt);	// Since updating pagerank with deltacounter result is done in	
    											// next mapper iteration, save the value in a new variable
    	
	   	if(niter == 2) {
	    	conf.setLong("stage", 1);	// This is for indicating initialization with 1/V is done at first stage
		    }
   	
	   	long convcnt = job1.getCounters().findCounter(MoreIterations.convergestatus).getValue(); // get the value from previous iteration
	   	
	   	if((convcnt - nV) >= 200) // if most of pages are not changing
	   		System.out.println("Looks like converging, need to observe more iterations");
	   	
	   	job1.getCounters().findCounter(MoreIterations.convergestatus).setValue(0); // set it for next observation
	   	
	   }    

	    // Job for Top100 Records
	    Job job2 = Job.getInstance(conf, "job2");
	    job2.setJarByClass(Preprocess.class);
	    job2.setMapperClass(TopKMapper.class);
	    job2.setReducerClass(TopKReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setMapOutputKeyClass(NullWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path(input));
	    FileOutputFormat.setOutputPath(job2, new Path(output));
	    code = job2.waitForCompletion(true) ? 0 : 1;


	    // return code;	
		System.exit(code);
	}
}
