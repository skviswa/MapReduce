package prank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import preprocess.Preprocess.MoreIterations;

// The only difference between matrix reducer and this is the way data is being read and written
// As this is a map only job

public class MatrixRowMapper extends Mapper<Object, Text, Text, Text>{

    HashMap<String,String> rankread = new HashMap<String,String>();
    List<String> dang = new ArrayList<String>();
    int rankflag = 0, dangflag = 0;

    @Override  
	protected void setup(Context context) throws IOException
	{

        long cur = context.getConfiguration().getLong("stage", 0);
        if(cur != 0) {

    		Configuration conf = context.getConfiguration(); 
    		String path = conf.get("output");
    		FileSystem fs = FileSystem.get(URI.create(path),conf);
    		int i;
    		FileStatus[] files = fs.listStatus(new Path(path));
    		rankflag = 1;
    		for(i=0;i<files.length;i++) {
    			if(!files[i].getPath().toString().contains("SUCCESS")) {
    				
    				BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(files[i].getPath())));
                    String pattern = fis.readLine();
                    while ((pattern != null)) {  // && pattern.length() > 0
                 		String[] data = pattern.split("\t");
                 		data[0] = data[0].replaceAll("\\s+","").replaceAll("\\t+","");
                  	if(data.length > 1)	{
                  		data[1] = data[1].replaceAll("\\s+","").replaceAll("\\t+","");
                  		rankread.put(data[0], data[1]);
                  	}
                  		
                  	pattern = fis.readLine();	
                    }
                    fis.close();
    			}
    		}	
    		fs.close();
    		
    		path = conf.get("D");
    		fs = FileSystem.get(URI.create(path),conf);
    		files = fs.listStatus(new Path(path));
    		dangflag = 1;
    		for(i=0;i<files.length;i++) {
    			
    			if(!files[i].getPath().toString().contains("SUCCESS")) {
    				
    				BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(files[i].getPath())));
    				String pattern = fis.readLine();
                while ((pattern != null && pattern.length() > 0)) {
             		pattern = pattern.replaceAll("\\s+","").replaceAll("\\t+","");
              	dang.add(pattern);
              	pattern = fis.readLine();	
            }
                fis.close();	
            }
    		}
        }

        	danglingmass(context);
        
        
     }
    private void danglingmass(Context context) {
    	long V = context.getConfiguration().getLong("numVertices", 0);
    	float r = 0;
		long cur = context.getConfiguration().getLong("stage", 0);

		if(cur == 0) {
			long d = context.getConfiguration().getLong("dangling", 0);
    		r  += (float) d/(V*V);
		}
		
		else {
    	if(rankflag == 1 && dangflag == 1) {
    		for (int i=0; i<dang.size(); i++) {
    			r += (float) Float.parseFloat(rankread.get(dang.get(i)))/V;
    		}
    		
    		r = (float)(0.85*r);	
    	}
		}
		   long cntparser = (long)(r * 100000);	// Precision of 10^5 to convert double to long
		   context.getCounter(MoreIterations.deltaCounter).increment(cntparser);	// Update delta counter as this is a dangling node

    }
    
    @Override  
	protected void cleanup(Context context) throws IOException {
   	 
   	 rankread.clear();	 
   	 dang.clear();
    }
    
    @Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	       long V = context.getConfiguration().getLong("numVertices", 0); // Get value of no of vertices
		   long cur = context.getConfiguration().getLong("stage", 0); 	// Find if this is initialization stage or not
		   float rank = (float) (0.15/V);
		   float rankvert= 0;
		   float contrib = 0;
		   
		   String[] res = value.toString().split("\t");
		   
		   if(res.length > 1) {
			   res[1] = res[1].replaceAll("\\s+","").replaceAll("\\t+","");
			   String[] cells = res[1].split(",");
			   for(int i=0; i<cells.length; i++) {
		   
				   String[] data = cells[i].split(":"); // Split and store in to an array of strings
			       contrib = Float.parseFloat(data[0]);
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
		   
		   long deltacnt = context.getCounter(MoreIterations.deltaCounter).getValue();
		   rank += (float) deltacnt/100000;   
		   res[0] = res[0].replaceAll("\\s+","").replaceAll("\\t+","");
	     context.write(new Text(res[0]), new Text(Float.toString(rank)));

	}
}
