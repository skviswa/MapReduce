package topk;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// This is the implementation of Top100 Reducer job
// I collect all the Top100 results of local mappers
// Store the results in the form of a TreeMap again
// Emit the final 100 results in descending order

public class TopKReducer extends Reducer<NullWritable,Text,Text,Text> {
	
	private static int N = 100;
	
    private static final TreeMap<Float, Text> FinalMap = new TreeMap <Float, Text>(); 

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
    	 
    	for(Float key : FinalMap.descendingKeySet()) {
    		  context.write(FinalMap.get(key), new Text(Float.toString(key)));
           } 
} //~cleanup

}
