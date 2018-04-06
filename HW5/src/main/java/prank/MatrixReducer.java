package prank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.StringUtils;

import preprocess.Preprocess.MoreIterations;

// IN HERE, WE SIMPLY GET ALL THE CONTRIBUTIONS FROM NON ZERO COLUMNS FOR EACH ROW,
// MULTIPLY THE PAGERANK OF THAT NODE WITH THE CONTRIBUTION IT GIVES TO THIS RECORD
// THE DANGLING MASS IS CALCULATED AFTER THE DANGLING LIST OF NODES AND THE 
// PAGE RANK FILES ARE LOADED IN SETUP, AND THEY ARE SHARED TO THE REDUCE TASK
// BY USE OF GLOBAL COUNTER

public class MatrixReducer extends Reducer<Text,Text,Text,Text> {

    HashMap<String,String> rankread = new HashMap<String,String>();			// This data structure reads the pagerank file
    List<String> dang = new ArrayList<String>();							// This data structure stores the list of dangling nodes
    int rankflag = 0, dangflag = 0;

    @Override  
	protected void setup(Context context) throws IOException
	{

        long cur = context.getConfiguration().getLong("stage", 0);
        if(cur != 0) {
        	// If this is not the first stage, we would have generated the rank file, so we can safely load it
    		Configuration conf = context.getConfiguration(); 
    		String path = conf.get("output");   // The path is passed to conf through this parameter
    		FileSystem fs = FileSystem.get(URI.create(path),conf);
    		int i;
    		FileStatus[] files = fs.listStatus(new Path(path));
    		rankflag = 1;
    		for(i=0;i<files.length;i++) {
    			if(!files[i].getPath().toString().contains("SUCCESS")) {
    				BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(files[i].getPath())));
                    String pattern = fis.readLine();
                    while ((pattern != null)) {  // && pattern.length() > 0
                 		String[] data = pattern.split(":");
                 		data[0] = data[0].replaceAll("\\s+","").replaceAll("\\t+","");
                  	if(data.length > 1)	{
                  		data[1] = data[1].replaceAll("\\s+","").replaceAll("\\t+","");
                  		rankread.put(data[0], data[1]);  // Read the rank and store it as key value pair
                  	}
                  		
                  	pattern = fis.readLine();	
                    }
                    fis.close();
    			}
    		}	
    		fs.close();
    		
    		path = conf.get("D");  // Once we have the ranks, we can next get the list of dangling nodes, whose path can be obtained from conf
    		fs = FileSystem.get(URI.create(path),conf);
    		files = fs.listStatus(new Path(path));
    		dangflag = 1;
    		for(i=0;i<files.length;i++) {
    			if(!files[i].getPath().toString().contains("SUCCESS")) {
    				BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(files[i].getPath())));
    				String pattern = fis.readLine();
                while ((pattern != null && pattern.length() > 0)) {
             		pattern = pattern.replaceAll("\\s+","").replaceAll("\\t+","");
              	dang.add(pattern);  // Load it in the data structure
              	pattern = fis.readLine();	
            }
                fis.close();	
            }
    		}
             

        	danglingmass(context);  // Once we have both data structures in memory, we can calculate
        }                           // Dangling mass right here 
        
     }
    private void danglingmass(Context context) {
    	long V = context.getConfiguration().getLong("numVertices", 0);  // Get number of vertices
    	float r = 0;
    	if(rankflag == 1 && dangflag == 1) {       // If both data structures are in memory
    		long cur = context.getConfiguration().getLong("stage", 0);  
    		if(cur == 0) {
        		r  += (float) dang.size()/(V*V);   // If its the first stage, then all the page ranks are 1/V
    		}
    		else {
    		for (int i=0; i<dang.size(); i++) {			
    			r += (float) Float.parseFloat(rankread.get(dang.get(i)))/V; // If not, get the page ranks of each dangling node and then add to the result
    		}
    		
    		}
    		r = (float)(0.85*r);	// Need to multiply by 0.85
    	}
    	
		   long cntparser = (long)(r * 100000);	// Precision of 10^5 to convert double to long
		   context.getCounter(MoreIterations.deltaCounter).increment(cntparser);	// Update delta counter as this is a dangling node

    }
    
     public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

       long V = context.getConfiguration().getLong("numVertices", 0); // Get value of no of vertices
	   long cur = context.getConfiguration().getLong("stage", 0); 	// Find if this is initialization stage or not
	   float rank = (float) (0.15/V);
	   float rankvert= 0;
     for(Text value : values) {
	   if(!value.toString().isEmpty()) { // If there is an adjacency list for this node
		   String[] data = value.toString().split(":"); // Split and store in to an array of strings
	       float contrib = Float.parseFloat(data[0]);
		   if(cur == 0) {	// If initializing (iteration 1)
			 rank += (float) 0.85*(contrib/V);
		   }
		   
		   else { 	// If this isnt the first iteration
			   
				   if(rankread.containsKey(data[1])) {
			    	   rankvert = Float.parseFloat(rankread.get(data[1]));
				
				       rank += (float) 0.85*contrib*rankvert;
				   }    
		   }
		   
		   }

     } 
     
       // Load the calculated dangling mass and add it to the result
	   long deltacnt = context.getCounter(MoreIterations.deltaCounter).getValue();
       rank += deltacnt/100000;

     context.write(key, new Text(":"+Float.toString(rank)));


   }
     
     @Override  
 	protected void cleanup(Context context) throws IOException {
    // Clear both data structures	 
    	 rankread.clear();	 
    	 dang.clear();
     }
     
}
     
