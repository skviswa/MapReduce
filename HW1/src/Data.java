import java.util.*;


// The main class which passes the arguments
public class Data {

    // Function to print runtime for each method
	public static void printruntime(List<Double> runtime) {
		double sum = 0;
		for (int i = 0; i<runtime.size(); i++) {
			sum += runtime.get(i);
		}
		System.out.println("Min: " + Collections.min(runtime) +
				" Max: " + Collections.max(runtime) + 
				" Average: " + (sum/runtime.size()));
	}

	// Main Function
	public static void main(String[] args) throws Exception {
    // args[0] is the input which points to location of file in memory
	// args[1] is the input which defines what type of sequential or multi-threaded
	// program is used to computes the average of TMax	
		
		// Define data structure to store the file information in memory
		List<String> data = new ArrayList<>();
		Boolean fib; // To run the Fibonacci(17) experiment

		
		if(args.length==1) {
			// Scanner object initialized and data is fetched.
	        CSVScanner s = new CSVScanner(args[0]);
	        data = s.getData();
	        fib = false; // default value is false
		}

		else {
		if(args.length==2) {
	        CSVScanner s = new CSVScanner(args[0]);
	        data = s.getData();
	        fib = Boolean.parseBoolean(args[1]);
		}

		else
			throw new IllegalArgumentException("Please give an input file!");
		}

        // Initialize an object that computes average of TMax in one of the 5 modes
 //       AvgTemp a = new AvgTemp(data, fib);  
 //       a.RunSeq();
        System.out.println("\n SEQUENTIAL IMPLEMENTATION \n");
 //       printruntime(a.runtime);
        
        AvgTempParallel p = new AvgTempParallel(data, fib);
        p.ParallelAvg();
        System.out.println("\n PARALLEL IMPLEMENTATION \n");
        printruntime(p.runtime);
        
        AvgTempCoarseLock c = new AvgTempCoarseLock(data, fib);
        c.ParallelAvg();
        System.out.println("\n COARSE-LOCK IMPLEMENTATION \n");
        printruntime(c.runtime);

        AvgTempFineLock f = new AvgTempFineLock(data, fib);
        f.ParallelAvg();
        System.out.println("\n FINELOCK IMPLEMENTATION \n");
        printruntime(f.runtime);
        
        AvgTempNoShared n = new AvgTempNoShared(data, fib);
        n.ParallelNoShare();
        System.out.println("\n NO-SHARING IMPLEMENTATION \n");
        printruntime(n.runtime);

        

	}

}
