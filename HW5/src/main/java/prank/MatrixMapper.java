package prank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// THIS FILE IS USED FOR COLUMN MAJOR FORMAT PROGRAM
// WE READ EACH RECORD STORED IN COLUMN MAJOR FORMAT,
// AND WE EMIT 
// ROW NO	CONTRIBUTION:COLUMN NO
// TO THE REDUCE PHASE

public class MatrixMapper extends Mapper<Object, Text, Text, Text>{
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] data = value.toString().split(":");
        int l = data.length;	
	    if(l>1) { // If there is an adjacency list for this node
	    	String[] adjs = data[2].split("~");
	    	data[0] = data[0].replaceAll("\\t+","").replaceAll("\\s+","");
			//context.write(new Text(data[0]), new Text(data[1]+":"+data[2]));
			for(int i=0; i<adjs.length; i++) {
				context.write(new Text(adjs[i]), new Text(data[1]+":"+data[0]));
			}
	    }
	    else {
	    	String k = value.toString().replaceAll("\\s+","").replaceAll("\\t+","");
	    	context.write(new Text(k), new Text(""));
	    }
}
}