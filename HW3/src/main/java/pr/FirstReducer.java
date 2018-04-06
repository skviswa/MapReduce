package pr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import pr.Preprocess.MoreIterations;

// This class collects all pagenames, discards the duplicates, initializes the pagerank to 0
//I then count the number of unique pagenames that are part of the linknames through HashMap global
//This is updated to a global counter numVertices to keep track of number of Vertices in the graph.
// HashMap local takes care of emitting only locally unique pagenames from the adjacency list
// Then transmit the output in a format that can be processed directly by the PageRank job



public class FirstReducer extends Reducer<Text,Text,Text,Text> {

	HashMap<String,Integer> global = new HashMap<String,Integer>();  // Global HashMap to count number of vertices

	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  HashMap<String,Integer> local = new HashMap<String,Integer>();
		  StringBuilder output = new StringBuilder();
		String k = key.toString();    
		if(!global.containsKey(k)) {
			global.put(k, 1);		// A reducer level HashMap to count number of vertices
		   context.getCounter(MoreIterations.numVertices).increment(1); // Global counter counts number of vertices
		}
		
	  
		  output.append(":0:");	// Initializing pagerank with 0 and adding a delimiter format to ensure easy separation for next job
		  for(Text v : values) {
			  String temp = v.toString();
			  temp = temp.replaceAll("\\[", "").replaceAll("\\]", ""); // Remove the square brackets from the list obtained through parser
			  if((!temp.isEmpty())) {
			  	  
				  String[] data = temp.split(",");
				  for(int i = 0; i < data.length; i++) {
					  data[i] = data[i].replaceAll("\\s+","");	// Remove any white spaces
					  if(!global.containsKey(data[i])) {
					  context.getCounter(MoreIterations.numVertices).increment(1); // Global counter to count no of vertices
					  }
					  if(!local.containsKey(data[i]))
					  output.append(data[i]+",");	// Only one local copy per unique adjacency list pagename is sent to the next job
					  local.put(data[i], 1);
					  global.put(data[i], 1);
				  }
			  }
			  else
				  output.append(",");
		  }	  
		  output.setLength(Math.max(output.length() - 1, 0));  // This is to remove the last comma while building string
		  
	  	  context.write(new Text(k), new Text(output.toString())); // Emit result of the form pagename :pagerank:adjacencylist(pagename1,pagename2,...)
		    
		  }

}
