import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object TestScala {
  def main(args:Array[String]):Unit = {
   val conf = new SparkConf()
      //            .setMaster("local")       // This is for the local run
                  .setAppName("PageRank")  // The application name is set
   val sc = new SparkContext(conf)
   
   val iters = if (args.length > 2) args(2).toInt else 10  // Number of iterations is input, default is 10
   
   val file = sc.textFile(args(0))                    // Take the input files 
   val b = new Bz2WikiParser()                       // Create a new parser object
   val data = file.map(line => b.parser(line))      // Parse line by line
                  .filter(f => f != null)          // Remove null results
   
   // Record is read in to data in the format pageName:AdjacencyList, and both are of type String
  // The records with no AdjacencyList are represented by the String "~"                 
 //  We can then proceed with creating a pairRDD for the same
                  
   val pairRDD = data.map(_.split(":"))                         // get the pageName
                     .keyBy(_(0))                              //  Set it to be the key       
                     .mapValues(f => f(1).split(",").toList)  // Save the AdjacencyList as a list from the String
                     .persist()                              // Persist the result
    
    val V = pairRDD.count() // Get a count of the number of pages in the record
                    
    val ranks = pairRDD.mapValues(v => (1.0/V).toFloat)  // Create a new pair RDD with initial page rank values
                       .persist()            
    
    var dangling = ranks.join(pairRDD)   // Create a new pairRDD by joining the RDDs with rank and adjacency list
    
    for (i <- 1 to iters) {
            
      val deltacnt = dangling.filter(f => {                      // Now there are 2 cases for dangling pages   
                     if(f._2._2 == null)                        // first is the ones that are part of collection that was
                         true                                  // identified in the parser, for which we explicitly stated adjacency list would be represented as "~"
                       else if (f._2._2(0).equals("~"))       // The other scenario is pages that are not a part of the collection but exist in adjacency list
                        true                                 // for which join result would be a null for adjacency list
                       else
                         false})                                
                     .map(f => ("DANGLING",f._2._1))        // We need the rank result of the value  
                     .reduceByKey(_ + _)                   // Sum everything up to produce the deltacounter value 
                     .lookup("DANGLING")(0)               // This is equivalent to a transformation
                     
//                     .mapValues(f => f._2)          // The same deltacounter value can be performed
//                     .map(_._2)                    // through an action by doing map to get the papge rank 
//                     .reduce(_ + _)               // and reduce to sum them up as shown
                     
      val newranks = dangling
                     .values                          // now here, we need to update the contributions of each node                   
                     .flatMap({ case (rank, urls) => // for dangling nodes, we just need to ignore contributions, which is done by setting it to zero
                     if(urls == null)
                       urls.map(url =>(url, 0.toFloat))
                                                                      
                     else if(urls(0).equals("~"))            
                      urls.map(url =>(url, 0.toFloat))        
                      
                     else                              // Collect the contributions for each page in adjacency list if its not a dangling node
                       urls.map(url => (url, (rank / urls.size).toFloat))
                     })                                     
                     .reduceByKey(_ + _)              // Sum up all contributions received by one pageName 
                     .mapValues(f => ((0.15/V) + (0.85*f) + (0.85*deltacnt/V)).toFloat)      //  And apply the pagerank formula, taking in to account contributions from dangling nodes
      
     if( i != 10)
     dangling = newranks.join(pairRDD)     // Join the newly calculated ranks and pairRDD RDDS                 
                                           
    }                  
   
                  // Sort the result in descending order of rank
    
   val result = sc.parallelize(
                  dangling
                  .sortBy(_._2._1,false)
                  .take(100)     // Take the top 100 values
                  .map(line => line._1 + " : " + line._2._1),1)     

   result.saveAsTextFile(args(1))     
    sc.stop()                     
  }
}
