import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MeanTempCombiner {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf().setMaster("local").setAppName("test1")
   val sc = new SparkContext(conf)

  val file = sc.textFile("/home/karthik/eclipse-workspace/MeanTempScala/input1/1885.csv")
  val data = file.map(line => line.split(","))                           // Split the CSV file in to strings
             .filter(f => (f(2).equals("TMAX") || f(2).equals("TMIN")))  // Filter according to TMAX or TMIN
             .map(v => ((v(0), v(2)), v(3).toLong))                     // Create a Map of type ((StationID,TMAX/TMIN),Temp Value)
             .persist()
//             .saveAsTextFile("/home/karthik/eclipse-workspace/MeanTempScala/output1")
  
  val mv = data.mapValues(f => (f,1))   // This adds 1 to each value with a key, thus introducing the concept of count
           .persist()
//           .saveAsTextFile("/home/karthik/eclipse-workspace/MeanTempScala/output") 
           
     // ReduceByKey acts like a combiner. The given x,y method does both combine and reduce.      
           
  val result = mv.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
               .persist() 
               .saveAsTextFile("/home/karthik/eclipse-workspace/MeanTempScala/output1")  
                       
               // Now for each key, we just divide sum by count to get average
  } 

}