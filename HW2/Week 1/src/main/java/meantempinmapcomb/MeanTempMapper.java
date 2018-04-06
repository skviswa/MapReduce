package meantempinmapcomb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MeanTempMapper extends Mapper<Object, Text, Text, Text>{

	public HashMap<String,List<Long>> combiner = new HashMap<String,List<Long>>(); // HashMap to keep track of Mapper class temperature data
	// The list of values correspond to Sum_max, count_max, sum_min, count_min respectively
@Override
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    long sum_min=0;
    long sum_max=0;
    long count_min=0;
    long count_max=0;
	List<Long> partial = new ArrayList<>();
	   String[] data = value.toString().split(","); // Split and store in to an array of strings
	   if(combiner.containsKey(data[0])) {  // First check the HashMap if partial data is already accumulated
           partial = combiner.get(data[0]);
           sum_max = partial.get(0);      // Retain the data 
	       count_max = partial.get(1);
	       sum_min = partial.get(2);
	       count_min = partial.get(3);

		   if(data[2].equals("TMAX")) {  // need to know whether to update TMax or TMIN
			   if(!data[3].isEmpty()) {
                   sum_max = partial.get(0) + Long.parseLong(data[3]);  // If TMAX, update TMAX and keep track of TMIN 
                   count_max = partial.get(1) + 1;
  		   }
		   }

		   if(data[2].equals("TMIN")) {
			     if(!data[3].isEmpty()) {
			     
                  sum_min = partial.get(2) + Long.parseLong(data[3]);     // If TMIN, update TMIN and keep track of TMAX
                  count_min = partial.get(3) + 1;
			     }
		      }
		   
	   }
	   else {  // Means new data is added
		   if(data[2].equals("TMAX")) {
			   if(!data[3].isEmpty()) {
				   sum_max = Long.parseLong(data[3]);  // Update with data
				   count_max = 1;
			   }
		   }

		   if(data[2].equals("TMIN")) {
			   if(!data[3].isEmpty()) { 
				   sum_min = Long.parseLong(data[3]);  // Update with datas
				   count_min = 1;
			   }
		   }

		   // No need to update max or min case as its default 0,0

	   }   

	    partial.clear();      //  In case list was filled, clear the list
	    partial.add(0,sum_max);        // Add elements according to the right order mentioned
	    partial.add(1,count_max);
	    partial.add(2,sum_min);
	    partial.add(3,count_min);
	    if(data[2].equals("TMAX") || data[2].equals("TMIN"))
        combiner.put(data[0], partial);  // Key is station ID and value is List of sum and count
	   
     }


@Override
public void cleanup(Context context) throws IOException, InterruptedException {
	for(String key : combiner.keySet()) {
		List<Long> p = combiner.get(key);  // For each key, emit the List of sum and count
		context.write(new Text(key), new Text(p.get(0)+","+p.get(1)+","+p.get(2)+","+p.get(3)));  // emit value of each key
	}
    combiner.clear();
}

 }
