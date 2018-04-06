package meantempcombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MeanTempReducer extends Reducer<Text,Text,Text,Text> {

      @Override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    long sum_min = 0;
	    int count_min = 0;
	    long sum_max =0;
	    int count_max = 0;
	    float avg_max,avg_min;
	    for (Text v : values) {
	      String[] data = v.toString().split(",");
	    		sum_max += Integer.parseInt(data[0]);  // Count max temp
	            count_max += Integer.parseInt(data[1]);
	    		  sum_min += Integer.parseInt(data[2]); // count min temp
	              count_min += Integer.parseInt(data[3]);    	  
	    } 
	    avg_max = 0;
	    avg_min = 0; 
	    
	    if(count_min != 0)
	        avg_min = (float)sum_min/count_min; // average exists only if there is a count
	        if(count_max != 0)
	        avg_max = (float)sum_max/count_max;
	        
	        // Emit result according to availability of record
	        if(count_max == 0) {
	        	if(count_min == 0) 
	        		context.write(key, new Text("No record available , No record available"));
	        	else 
	        		context.write(key, new Text(avg_min + " , No record available"));
	        	
	        }
	        else {
	        	if(count_min == 0)
	        		context.write(key, new Text("No record available , " + avg_max));
	        
	        	else	
	                context.write(key, new Text(avg_min + " , " + avg_max));
	        }
	  }
	}