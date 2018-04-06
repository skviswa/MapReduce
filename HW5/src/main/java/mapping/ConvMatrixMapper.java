package mapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// HERE, WE TAKE THE ADJACENCY LIST OF EACH RECORD, IF IT EXISTS, 
// AND APPLY THE MAPPING TO CONVERT EVERYTHING TO NUMBERS
// THE FINAL RECORD EMITTED IN THIS MAP ONLY JOB IS A MATRIX IN COLUMN MAJOR FORM,
// COLUMN NO	CONTIRBUTION,ADJACENCY LIST OF (ROW NUMBERS)

public class ConvMatrixMapper extends Mapper<Object, Text, Text, Text>{
	
    HashMap<String,String> urlmap = new HashMap<String,String>();  // LOAD THE NUMBER TO PAGENAME MAPPING IN THIS HASHMAP

    @Override  
	protected void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration(); 
		String path = conf.get("matmap");
		FileSystem fs = FileSystem.get(URI.create(path),conf);
		// READ THE SAVED MAPPING FROM THE FILE SYSTEM
		FileStatus[] files = fs.listStatus(new Path(path));

		for(int i=0;i<files.length;i++) {
			if(!files[i].getPath().toString().contains("SUCCESS")) {
				BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(files[i].getPath())));
                String pattern = fis.readLine();
                while ((pattern != null)) {  // && pattern.length() > 0
             		String[] data = pattern.split("\t");
             		data[0] = data[0].replaceAll("\\s+","").replaceAll("\\t+","");
              	if(data.length > 1)	{
              		data[1] = data[1].replaceAll("\\s+","").replaceAll("\\t+","");
              		urlmap.put(data[0], data[1]); // LOAD THE HASHMAP
              	}
              		
              	pattern = fis.readLine();	
                }
                fis.close();
			}
		}	


        }
    
    @Override  
	protected void cleanup(Context context) throws IOException
	{
    	urlmap.clear();
	}
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		  float contrib;	
		  String no = "";
		  StringBuilder output = new StringBuilder();
			  String temp = value.toString();
			  output.append("");
			  String[] res = temp.split("\t");
			  if(res.length > 1) {
				 
				  String[] data = res[1].split("~");
				  contrib = (float)1/(data.length);
				  output.append(":"+Float.toString(contrib)+":");
				  for(int i = 0; i < data.length; i++) {
					 no = urlmap.get(data[i]);
					 if(no == null)
						 System.err.println("CACHE FILE NOT READ");
					  output.append(no+"~");   // APPLY THE MAPPING AND APPEND THE RESULT
				  }
			 output.setLength(Math.max(output.length() - 1, 0));  // This is to remove the last comma while building string
			 }
			  
	  	  context.write(new Text(res[0]), new Text(output.toString())); 	
	}
}
