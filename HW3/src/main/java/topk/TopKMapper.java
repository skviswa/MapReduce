package topk;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// This implements the Top 100 records Mapper job
// I use a TreeMap to store the top 100 paegranks and its associated pagenames
// I then emit the result as a NULLWRITABLE key along with pagename and pagerank as value

public class TopKMapper extends Mapper<Object, Text, NullWritable, Text>{
	


	private static int N = 100;
	
	private TreeMap<Float, Text> TopKMap = new TreeMap<Float, Text>();
    
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	   String[] data = value.toString().split(":") ;
	
	   Float pagerank = Float.parseFloat(data[1]);
	   String k = data[0].replaceAll("\\s+","");
	   TopKMap.put(new Float(pagerank), new Text(k+":"+pagerank));
	
	   if (TopKMap.size() > N) {
	      TopKMap.remove(TopKMap.firstKey());
	   }

} //~map

    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for (Float k : TopKMap.keySet()) {
    		Text value = TopKMap.get(k);
    		context.write(NullWritable.get(), value);
} 
} 


}
