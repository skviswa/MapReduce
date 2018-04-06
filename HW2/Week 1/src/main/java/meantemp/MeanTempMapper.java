package meantemp;

import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MeanTempMapper extends Mapper<Object, Text, Text, Text>{

@Override
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	   String temp = new String();
	   String[] data = value.toString().split(","); // Split and store in to an array of strings
	//   try {
		   if((data[2].equals("TMAX")) || (data[2].equals("TMIN"))) {
			   if(data[3].isEmpty())
				   temp = "NA";  // If record is missing, indicate by NA 
			   else
				   temp = data[3];
		       String val = data[2] + "," + temp;  // Else store the temperature in a string
		   
	//   } catch (Exception e) {
//		   e.addSuppressed(null);
	//   }
       context.write(new Text(data[0]), new Text(val));  // Key is station ID and value is temperature
      }
     }
 }


