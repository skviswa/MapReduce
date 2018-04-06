package meantempcombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MeanTempMapper extends Mapper<Object, Text, Text, Text>{

         
	  // @SuppressWarnings("null")
	@Override
	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		   
		   String[] data = value.toString().split(",");
		   String temp = new String();
		   
		//   try {
			   if((data[2].equals("TMAX")) || (data[2].equals("TMIN"))) {
			       	 if(data[3].isEmpty())
			       		 temp = "NA";
			       	 else
			       		 temp = data[3];
			       	 
			   
		//   } catch (Exception e) {
//			   e.addSuppressed(null);
		//   }
	       context.write(new Text(data[0]) , new Text(data[2] + "," + temp + "," + "1"));
	      } 
	     }
	   
	 }
