package meantempcombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MeanTempCombiner extends Reducer<Text,Text,Text,Text> {

	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    long sum_min = 0;  // Keep track of partial max temp
	    int count_min = 0;
	    long sum_max =0;  // keep track of partial min temp
	    int count_max = 0;
 	    
	    for (Text v : values) {
	      String[] data = v.toString().split(",");
	      if(data[0].equals("TMAX")) {
	    	  if(data[1].equals("NA"))  // Check for record not available. If so skip transmission.
	    		  continue;
	          sum_max += Long.parseLong(data[1]);  // keep track of partial sum for max temp
	          count_max += Integer.parseInt(data[2]);;
	         }
	    
		      if(data[0].equals("TMIN")) {
		    	  if(data[1].equals("NA"))
		    		  continue;
		          sum_min += Long.parseLong(data[1]);   // keep track of partial sum for min temp
		          count_min += Integer.parseInt(data[2]);;
		         }
		}
	    // sufficient if we transmit the 4 calculated values as we know the categories now
	    context.write(key, new Text(sum_max+","+count_max+","+sum_min+","+count_min));
}
	  
}	  