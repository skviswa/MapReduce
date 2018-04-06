package meantemp;

//import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// This class is to calculate mean min. and max. Temperature without use of Combiner 
public class MeanTemp { 


public static void main(String[] args) throws Exception {
	// TODO Auto-generated method stub
	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MeanTemp");
    job.setJarByClass(MeanTemp.class);
    job.setMapperClass(MeanTempMapper.class);
  //  job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(MeanTempReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}
	
}
