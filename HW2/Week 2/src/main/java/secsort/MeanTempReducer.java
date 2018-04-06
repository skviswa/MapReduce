package secsort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MeanTempReducer extends Reducer<Text,Text,Text,Text> {

    @Override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	HashMap <Integer,List<Long>> res = new HashMap<Integer,List<Long>>(); // Hashmap keeps track of total sum and count
    	                                                                      // for every year
    	
	    long sum_min = 0;                      // Keep track of min temp sum and count
	    long count_min = 0;
	    long sum_max =0;					// keep track of max temp sum and count
	    long count_max = 0;
	    float avg_max,avg_min;       
	    int year;
	    String[] outkey = key.toString().split(",");   // The key is split in to Station ID and year
	    List<Long> partial = new ArrayList<>();
      for (Text v : values) {
    	  
	      String[] data = v.toString().split(",");
	           year = Integer.parseInt(data[4]);  // Year info is transmitted with data
	           if(res.containsKey(year)) {
	        	   partial = res.get(year);
		           sum_max = Long.parseLong(data[0]) + partial.get(0);  // Count max temp
		           count_max = Long.parseLong(data[1]) + partial.get(1);
		    	   sum_min = Long.parseLong(data[2]) + partial.get(2); // count min temp
		           count_min = Long.parseLong(data[3]) + partial.get(3);    	  
	           }
	           else {
		           sum_max = Long.parseLong(data[0]);  // Count max temp
		           count_max = Long.parseLong(data[1]);
		    	   sum_min = Long.parseLong(data[2]); // count min temp
		           count_min = Long.parseLong(data[3]);    	  
	           }
	           partial.clear();                 // Since its linked list, clear the list 
			   partial.add(0, sum_max);                    
			   partial.add(1, count_max);
			   partial.add(2, sum_min);
			   partial.add(3, count_min);
			   res.put(year, partial);  // Add updated value to HashMap
      }
	    avg_max = 0;
	    avg_min = 0; 
	    Object[] yr = res.keySet().toArray();
	    Arrays.sort(yr);
	    
	    StringBuilder output = new StringBuilder();   // This creates the record style that we are looking for.
	    output.append("[");
	    for(Object y : yr) {
		    if(res.get(y).get(3) != 0)
		        avg_min = (float)res.get(y).get(2)/res.get(y).get(3); // average exists only if there is a count
		    if(res.get(y).get(1) != 0)
		        avg_max = (float)res.get(y).get(0)/res.get(y).get(1); // average exists only if there is a count
		    
	        if(res.get(y).get(1) == 0) {
	        	if(res.get(y).get(3) == 0) // Emit station ID as final key and year along with average as final value
	        		output.append(("("+y+": "+"No record available , No record available), "));
	        	else 
	        		output.append(("("+y+": "+avg_min + " , No record available), "));
	        	
	        }
	        else {
	        	if(res.get(y).get(3) == 0)
	        		output.append(("("+y+": "+"No record available , " + avg_max+"), "));
	        
	        	else	
	        		output.append(("("+y+": "+": "+avg_min + " , " + avg_max+"), "));
	        }		    
	    	
	    }
	    output.append("]");
	        
	    context.write(new Text(outkey[0]), new Text(output.toString()));   // Emit result according to availability of record

	  }
	}