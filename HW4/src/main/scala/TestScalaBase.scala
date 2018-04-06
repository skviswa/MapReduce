import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object TestScalaBase {
  def main(args:Array[String]):Unit = {
   val conf = new SparkConf()
      //            .setMaster("local")       // This is for the local run
                  .setAppName("PageRank")  // The application name is set
   val sc = new SparkContext(conf)
   
   val iters = if (args.length > 2) args(2).toInt else 10  // Number of iterations is input, default is 10
   
//  val file = sc.textFile("/home/karthik/eclipse-workspace/PageRankSpark/input/wikipedia-simple-html.bz2")
   val file = sc.textFile(args(0))                    // Take the input files 
   val b = new Bz2WikiParser()                       // Create a new parser object
   val data = file.map(line => b.parser(line))      // Parse line by line
                  .filter(f => f != null)          // Remove null results
                //  .saveAsTextFile(args(1))
                //  .persist()
   
   // Record is read in to data in the format pageName:AdjacencyList, and both are of type String
  // The records with no AdjacencyList are represented by the String "~"                 
 //  We can then proceed with creating a pairRDD for the same
                  
   val pairRDD = data.map(_.split(":"))                         // get the pageName
                     .keyBy(_(0))                              //  Set it to be the key       
                     .mapValues(f => f(1).split(",").toList)  // Save the AdjacencyList as a list from the String
    //                 .mapPartitions(f, preservesPartitioning)
                     .persist()                              // Persist the result
                     
    val V = pairRDD.count() // Get a count of the number of pages in the record
                    
    val ranks = pairRDD.mapValues(v => (1.0/V).toFloat)  // Create a new pair RDD with initial page rank values
                       .persist()            
    var dangling = pairRDD.join(ranks)
    
    for (i <- 1 to iters) {
      
  //    var deltacnt = dangling           
  //                   .filter(f => (f._2._1)(0).equals("~"))  // We need the value of every key
  //                   .map(f => ("DANGLING",f._2._2))        // We need the rank result of the value  
  //                   .reduceByKey(_ + _)                   // Sum everything up to produce the deltacounter value 
  //                   .lookup("DANGLING")(0)               // This is equivalent to a transformation
                     
//                     .mapValues(f => f._2)          // The same deltacounter value can be performed
//                     .map(_._2)                    // through an action by doing map to get the papge rank 
//                     .reduce(_ + _)               // and reduce to sum them up as shown
                     
      val newranks = //dangling
                     pairRDD.join(ranks)                                       // Now join the ranks with the full dataset
                     .values                                                  // No need to update the dangling nodes
                     .flatMap{ case (urls, rank) => val size = urls.size     // Get only the values 
                     urls                                                   // For each page in the adjacency list,
                     .filter(url => !url.equals("~"))
                     .map(url => (url, (rank / size).toFloat))}        // Send the contribution it receives from pageName
                     .reduceByKey(_ + _)       
                     .mapValues(f => ((0.15/V) + (0.85*f)).toFloat)                                     // Sum up all the received contributions
   
      if(i != 10)
        dangling = pairRDD.join(newranks)
    }  
                  // Sort the result in descending order of rank
    
   val result = sc.parallelize(
                  dangling
                  .sortBy(_._2._2,false)
                  .take(100)     // Take the top 100 values
                  .map(line => line._1 + " : " + line._2._2),1)     
                  
    result.saveAsTextFile(args(1))     
    sc.stop()                     
  }
}
