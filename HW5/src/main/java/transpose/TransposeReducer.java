package transpose;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// 	HERE, WE JUST AGGREGATE THE RESULT FOR EACH ROW
// AND EMIT IT AS THE RESULT
// THE FINAL OUTPUT IS A MATRIX STORED IN ROW MAJOR FORMAT
// ROW NO	ADJACENCY LIST OF (CONTRIBUTION,COLUMN NO)

public class TransposeReducer extends Reducer<Text,Text,Text,Text> {
   
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuilder output = new StringBuilder();
      int count = 0;
      output.append("");
      for(Text value : values) {
 	   if(!value.toString().isEmpty()) // If there is an adjacency list for this node
 	   {  output.append(value.toString()+",");
 	   	  count++;	
 	   }
 		   
      } 
      if(count > 0) {
 	  output.setLength(Math.max(output.length() - 1, 0));
      context.write(key, new Text(output.toString()));
      }
      else
    	  context.write(key, new Text(""));

    }
}
