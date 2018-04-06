package meantemp;

import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MeanTempReducer extends Reducer<Text,Text,Text,Text> {

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    long sum_min = 0;  // For keeping track of min temp
    int count_min = 0;
    long sum_max =0;  // for keeping track of max temp
    int count_max = 0;
    for (Text v : values) {                       
      String[] data = v.toString().split(",");
      if(data[0].equals("TMAX")) {    // Check for TMAX or TMIN
    	  if(data[1].equals("NA"))   // Check whether value is available
    		  continue;
          sum_max += Integer.parseInt(data[1]); // Add to sum
          count_max++;
         }
      else {
    	  if(data[1].equals("NA"))  // Check whether value is available
    		  continue;
          sum_min += Integer.parseInt(data[1]);  // Add to sum
          count_min++;    	  
      }
    }
    float avg_min = 0;
    float avg_max = 0;
    if(count_min != 0)
    avg_min = (float)sum_min/count_min;  // average exists only if count is non zero
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


