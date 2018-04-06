import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object MeanTempSecSrt {
   def main(args: Array[String]): Unit = {
   val conf = new SparkConf().setMaster("local").setAppName("test1")
   val sc = new SparkContext(conf)

  val file = sc.textFile("/home/karthik/eclipse-workspace/MeanTempScala/input/*")
  val data = file.map(line => line.split(","))                          // Split the CSV file in to strings
             .filter(f => (f(2).equals("TMAX") || f(2).equals("TMIN")))  // Filter according to TMAX or TMIN
             .map(v => ((v(0), v(1).substring(0, 4), v(2)), v(3).toLong))                     // Create a Map of type ((StationID,TMAX/TMIN),Temp Value)
             .persist()
//             .saveAsTextFile("/home/karthik/eclipse-workspace/MeanTempScala/output1")
  
  // Aggregate groups locally and then groups globally. The x,y method does local grouping
             // The p,q method does the global grouping

  val processed_data = data.aggregateByKey((0.toLong,0))({(x,y) => (x._1+y,x._2+1)}, {(p,q) => (p._1+q._1,p._2+q._2)})
               .persist()  
  
   // Once aggregated, group by StationID first, then sort by the key (StationID,Year)            
               
  val result = processed_data.groupBy(f => f._1)                         
                       .sortBy(f => (f._1._1,f._1._2), true, 3)      
                       .persist() 
                       .saveAsTextFile("/home/karthik/eclipse-workspace/MeanTempScala/output3") 
                            
             // Now for each key, we just divide sum by count to get average        
 } 

}