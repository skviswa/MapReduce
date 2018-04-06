package prank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pr.Preprocess.MoreIterations;

// The pagerank reducer simply collects the ranks and the node object for each page
// It then implements the page rank algorithm without adding the contribution of dangling nodes
// As that would be shifted to the mapper on next iteration
// There is a condition where there can be no node object associated with a certain key
// because the page is not a part of the collection, though it is a part of the adjacency list of some page
// In that scenario, I create a new node object treating it as a dangling node and emit that result



public class PageRankReducer extends Reducer<Text,Text,Text,Text> {
	
	  private static float precision = (float) 0.000004; // an epsilon to check convergence  
	
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		  long V = context.getConfiguration().getLong("numVertices", 0); 	// Get number of vertices
		  String nodestr = ""; // Initially empty node object
		  StringBuilder output = new StringBuilder();
		  float s = 0;
		  for(Text val : values) {
			  if(val.toString().contains(":")) {  		
				  nodestr = val.toString();  // if you find the node object, save it	
			  }
			  else {
				  s += Float.parseFloat(val.toString());  //add the probabilities		
			  }
		  }
		  
		  if(!nodestr.isEmpty()) { // Check if there is a node object, taking care of condition explained in the beginning
		  String[] data = nodestr.split(":");
		  
		  s = ((float)(0.15/V) + (float) (0.85 * s));
		  
		  float prev = Float.parseFloat(data[0]); // get previous pagerank
		  if((s - prev) < precision)  	// check if this difference is lesser than precision
			  context.getCounter(MoreIterations.convergestatus).increment(1); // if so increment precision
		  
		  int l = data.length;
		  if(l==2)
		  output.append(":"+s+":"+data[1]);
		  else
			  output.append(":"+s+":"); // If no adjacency list present, this would be the output
		  context.write(key, new Text(output.toString()));
		  }
		  else {
			  output.append(":"+s+":");	// Create a node object for case discussed in the beginning above
			  context.write(key, new Text(output.toString()));
		  
		  }
		  }


}
