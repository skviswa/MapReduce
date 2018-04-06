package secsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.Math; 

public class MeanTempWithYr {

	  public static class YearPartitioner  // To partition a station id to each reducer
	    extends Partitioner<Text, Text> {

	    @Override
	    public int getPartition(Text key, Text value, int numPartitions) {
	      // Take hashcode of station id. We want each reducer to get one station id.
	      String stationid = key.toString().split(",")[0];
	      return ((Math.abs(stationid.hashCode()) & Integer.MAX_VALUE) % numPartitions);

	    }
	  }
	  
	  public static class KeyComparator extends WritableComparator {  // Sorts the keys
	    protected KeyComparator() {
	      super(Text.class, true);
	    }
	    @Override    // Sorting done in ascending order of key first, followed by ascending order of year for each station id
	    public int compare(WritableComparable w1, WritableComparable w2) {
	      Text ip1 = (Text) w1;
	      Text ip2 = (Text) w2;
	      String[] key1 = ip1.toString().split(",");
	      String[] key2 = ip2.toString().split(",");
	      Text temp1 = new Text(key1[0]);   // First sort by station id
	      Text temp2 = new Text(key2[0]);
	      int cmp = temp1.compareTo(temp2);
	      if(cmp != 0)
	    	  return cmp;
	      Text yr1 = new Text(key1[1]);
	      Text yr2 = new Text(key2[1]);   // Then sort by year if station id is same.
	      return yr1.compareTo(yr2);
	   
	    }
	  }   
	  // Grouping comparator. All the year stamps with respect to a particular station id are sent to same reduce call
	  public static class GroupComparator extends WritableComparator {
	    protected GroupComparator() {
	      super(Text.class, true);
	    }
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
		      Text ip1 = (Text) w1;
		      Text ip2 = (Text) w2;
		      String key1 = ip1.toString().split(",")[0];  // If station id is same, return that alone
		      String key2 = ip2.toString().split(",")[0];
		      Text temp1 = new Text(key1);
		      Text temp2 = new Text(key2);
		      return temp1.compareTo(temp2);
            //  return 0; 

	    }
	  }

	
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "MeanTempwithYear");
	    job.setJarByClass(MeanTempWithYr.class);
	    job.setMapperClass(MeanTempMapper.class);
	  //  job.setCombinerClass(IntSumReducer.class);
	    /*[*/job.setPartitionerClass(YearPartitioner.class);/*]*/
	    /*[*/job.setSortComparatorClass(KeyComparator.class);/*]*/
	    /*[*/job.setGroupingComparatorClass(GroupComparator.class);/*]*/
	    job.setReducerClass(MeanTempReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
