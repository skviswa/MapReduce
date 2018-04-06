package preprocess;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import preprocess.Preprocess.MoreIterations;

public class FirstReducer extends Reducer<Text,Text,Text,Text> {

	HashMap<String,Integer> global = new HashMap<String,Integer>();  // Global HashMap to map each pageName key to a unique number
	int urlno; 														// Global counter to generate numbers for each pageName		
	private MultipleOutputs<Text,Text> multoutput;					// multi output handle 
	
	  @Override  
	  public void setup(Context context)
	  {
	      multoutput = new MultipleOutputs<Text,Text>(context);  // Initialize the multi output handle class
	      urlno = 1;                                             // Initialize the global counter to 1
	      
	  }
	  
	  @Override
	  public void cleanup(Context context) throws IOException, InterruptedException
	  {
	      multoutput.close();   // Close the multiple output handle
	      global.clear();      // Clear global data structure
	  }		  
	
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String mfilename = context.getConfiguration().get("mfile");
        context.getCounter(MoreIterations.numVertices).increment(1); // Global counter counts number of vertices
        String outfile1,outfile2,outfile3;
		String k = key.toString();
		if(!global.containsKey(k)) {
			global.put(k, urlno);		
			urlno++;
		}


		outfile1 = mfilename+"Mapping/map";  // Save the Mapping {Number -> pageName}
		outfile2 = mfilename+"MatMap/url";  // Save the Mapping {pageName 
		outfile3 = mfilename+"D/dangling";  // Save the dangling nodes
		
		multoutput.write("Mapping", new Text(global.get(k).toString()), key, outfile1);
		multoutput.write("URL", key, new Text(global.get(k).toString()), outfile2);
		  int count = 0;
		  for(Text v : values) {
			  
			  String temp = v.toString();
			  if(temp.isEmpty())
				  continue;
			  else {
				  context.getCounter(MoreIterations.nondangling).increment(1);
					  count++;
					  context.write(new Text(global.get(k).toString()), v);	// Emit whatever we get as it is as we have already mapped the 
				  }															// pageNames to numbers and saved the mappings	
			  
		  }		  
		  	if(count == 0) {  
		  		multoutput.write("D", new Text(global.get(k).toString()), new Text(""), outfile3);				  
                context.write(new Text(global.get(k).toString()), new Text("")); 
		  }
	  }	  	

}
