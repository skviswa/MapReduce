package topk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.StringUtils;


// This is the implementation of Top100 Reducer job
// I collect all the Top100 results of local mappers
// Store the results in the form of a TreeMap again
// Emit the final 100 results in descending order 
// by reading the mapping file and converting the corresponding
// numbers to their equivalent pageNames

public class TopKReducer extends Reducer<NullWritable,Text,Text,Text> {
	
	private static int N = 100;
	
    private static final TreeMap<Float, Text> FinalMap = new TreeMap <Float, Text>(); 
    
    HashMap<String,String> urlmap = new HashMap<String,String>();

    @Override  
	protected void setup(Context context) throws IOException
	{
 
		Configuration conf = context.getConfiguration(); 
		String path = conf.get("mapping");
		FileSystem fs = FileSystem.get(URI.create(path),conf);
		
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
              		urlmap.put(data[0], data[1]);
              	}
              		
              	pattern = fis.readLine();	
                }
                fis.close();
			}
		}	
  	
 /*   	URI[] cachefiles = context.getCacheFiles();
  * 	FileSystem fs = FileSystem.get(context.getConfiguration());
        if(cachefiles != null && cachefiles.length > 0) {
        for (URI f : cachefiles) {
        	System.err.println("The URI of Mapping file in Cache: "+f.toString());

        	Path fpath = new Path(f.toString());
            try {

            	if(f.toString().contains("Mapping")) {
            		//BufferedReader fis = new BufferedReader(new FileReader(f.getPath()));	
                BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(fpath)));
                String pattern = fis.readLine();
                while ((pattern != null && pattern.length() > 0)) {
             		String[] data = pattern.split("\t");
             		data[0] = data[0].replaceAll("\\s+","").replaceAll("\\t+","");
              	if(data.length > 1)	{
              		data[1] = data[1].replaceAll("\\s+","").replaceAll("\\t+","");
              		urlmap.put(data[0], data[1]);
              	}
              	pattern = fis.readLine();	
                }
                fis.close();
              } 
              } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                    + StringUtils.stringifyException(ioe));
              }
              
            
          }
        	
          }
*/          
     }

    @Override
    public void reduce (NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

           for (Text value : values) {
        	   String[] data = value.toString().split(":");
               FinalMap.put(Float.parseFloat(data[1]), new Text(data[0]));
               if (FinalMap.size() > N) {
                  FinalMap.remove(FinalMap.firstKey()) ;
               }
           }


    } //~reduce

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	String outputkey = "";
    	for(Float key : FinalMap.descendingKeySet()) {
    		  outputkey = urlmap.get(FinalMap.get(key).toString());
    		  context.write(new Text(outputkey), new Text(Float.toString(key)));
           } 
} //~cleanup

}
