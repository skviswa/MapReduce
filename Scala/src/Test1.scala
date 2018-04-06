//import scala.tools.nsc.interpreter.Completion.ScalaCompleter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Test1 {
  def main(args: Array[String]): Unit = {
   val sc = new SparkConf().setMaster("local").setAppName("test1")
   val scx = new SparkContext(sc)
   
   val file = scx.textFile("/home/karthik/eclipse-workspace/MeanTempScala/input/1778.csv")
   //file.take(5).foreach(println) sqlc
   
   file.map(valu => (valu.split(",")(0), valu.split(",")(1))).saveAsTextFile("/home/karthik/eclipse-workspace/MeanTempScala/output2")
 
  }
}