package prank;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pr.Preprocess.MoreIterations;

// This class implements the iterative pagerank algorithm
// In the mapper, I have divided the code in two portions
// The first portion is to check if this is the first iteration and if so do initialization (1/V)
// If it is not the first iteration, update the page rank with old delta counter value,
// And increment the new datacounter value for the dangling nodes
// Emit result of the form pagename :pagerank:adjacencylist(pagename1,pagename2,...)



public class PageRankMapper extends Mapper<Object, Text, Text, Text>{

	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		   String[] data = value.toString().split(":"); // Split and store in to an array of strings
		   long V = context.getConfiguration().getLong("numVertices", 0); // Get value of no of vertices
		   int l = data.length;	
		   if(l>2) { // If there is an adjacency list for this node
		   String[] adjacents = data[2].split(","); // split the adjacency list as it exists.
		   long cur = context.getConfiguration().getLong("stage", 0); 	// Find if this is initialization stage or not
		   StringBuilder output = new StringBuilder();
		   int N;
		   float pagerank,contrib;	// calculate new pagerank and out contributions
		   
		   if(cur == 0) {	// If initializing (iteration 1)
			 
			   pagerank = (float)1/V;
			   output.append(pagerank+":"+data[2]);
			   String k = data[0].replaceAll("\\s+","");
			   context.write(new Text(k), new Text(output.toString())); // Write out the node as it is with an updated pagerank
				 N = adjacents.length;
				 
				 for(int i=0; i<N; i++) {
					 contrib = pagerank/N; // Contribution is coming through length of adjacency list
					 context.write(new Text(adjacents[i]), new Text(Float.toString(contrib))); // Now send out contributions to each neighbour
				 }
			   
		   }
		   
		   else { 	// If this isnt the first iteration
			   long delta;
			   delta = context.getConfiguration().getLong("olddeltaCounter", 0); // Get delta counter from last iteration
			   pagerank = Float.parseFloat(data[1]);
			   pagerank += (float)((float)((delta/100000)/V))*0.85; // Update pagerank with the delta counter value
			   output.append(pagerank+":"+data[2]);	// emit node data as such
			   String k = data[0].replaceAll("\\s+","");
			   context.write(new Text(k), new Text(output.toString())); //output.toString());
				 N = adjacents.length;
				 for(int i=0; i<N; i++) {
					 contrib = pagerank/N;
  					 context.write(new Text(adjacents[i]), new Text(Float.toString(contrib))); //emit contributions if there is an adjacency
				 }
			   
		   }
		   
		   }
		   else { // If there is no adjacency list associated with the node
			   long cur = context.getConfiguration().getLong("stage", 0);
			   float pagerank;
			   
			   if(cur == 0) { // If this is first iteration
				 
				   pagerank = (float)1/V;
				   String k = data[0].replaceAll("\\s+","");
				   context.write(new Text(k), new Text(Float.toString(pagerank)+":")); //emit the node after initializing rank
			   
			   }
			   else {
				   long delta;
				   delta = context.getConfiguration().getLong("olddeltaCounter", 0); // get delta counter from last iteration
				   pagerank = Float.parseFloat(data[1]);
				   pagerank += (float)((float)((delta/100000)/V))*0.85; // Update pagerank
				   String k = data[0].replaceAll("\\s+","");
				   context.write(new Text(k), new Text(Float.toString(pagerank)+":")); //emit the node with updated pagerank
				   
			   }
			   long cntparser = (long)(pagerank * 100000);	// Precision of 10^5 to convert double to long
			   context.getCounter(MoreIterations.deltaCounter).increment(cntparser);	// Update delta counter as this is a dangling node

	      
		   }

}
}