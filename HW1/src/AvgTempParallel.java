import java.lang.Runnable;
import java.util.*;
import java.lang.Math;


public class AvgTempParallel implements Runnable {

	// Data Structure to hold list of sum and count per station id
	// Key is station id and the list of temperature is the value
	private static List<String> data = new ArrayList<>();
	private static volatile HashMap<String, int[]> tdata = new HashMap<String, int[]>();
    
	// Contains final avg result per station id
	private HashMap<String, Float> avg = new HashMap<String, Float>();

	// Indices tracked so that data can be distributed to threads
	private int start_i;
	private int end_i;
	
	static Boolean fib;
	
	// For calculating time per run
     double t;

     // To hold and compute the average
	 List<Double> runtime = new ArrayList<>();

	
    // Function to print result
	public void printresult() {
	    Iterator<Map.Entry<String, Float>> i = avg.entrySet().iterator();
	    while (i.hasNext()) {
           Map.Entry<String, Float> entry = i.next();
	       System.out.println(entry.getKey() + " : " + entry.getValue());
	    }
	}

	// Converts the Partial sum and no of items count to the average for each station ID
	public void iter() {
        int temp[] = new int[2];
        float average;
	    Iterator<Map.Entry<String, int[]>> i = tdata.entrySet().iterator();
	    while (i.hasNext()) {
	        Map.Entry<String, int[]> entry = i.next();
	     //   System.out.println(entry.getKey() + ":" + entry.getValue());
            temp = entry.getValue();
            average = (float)temp[0]/temp[1];
            avg.put(entry.getKey(), average);
	    }
	}
	
	// Function to convert input data in to our accumulated data structure
		private void AccData() {
		Fibonacci ft = new Fibonacci();	
		int i = start_i;
		String s = data.get(start_i);
		int j = 0;

		while(i <= end_i) {
			s = data.get(i);
			String[] split = s.split(",");
			if(split[j+2].equals("TMAX")) { // If the line contains TMAX
     
       		    if(split[j+3].isEmpty())  // Skip if no data there
                 continue;
		        int temp[] = new int[2];
	
				if(tdata.containsKey(split[j])) { // If partial data present
					
            	  temp = tdata.get(split[j]); // Add info
            	  temp[0] = temp[0] + Integer.parseInt(split[j+3]);
            	  temp[1] = temp[1] + 1;
            	  if(fib)
            		  ft.f();  // for fibonacci(17)
            	  tdata.put(split[j], temp);
   			    }
				else {
	            	  temp[0] = Integer.parseInt(split[j+3]); // Else enter new info
	            	  temp[1] = 1;
	            	  if(fib)
	            		  ft.f();
	               	  tdata.put(split[j], temp);
				}

		 }
            
			i++;
		}
		
	}
	
		// Thread function
	@Override	
	public void run() {
	
		AccData();
	}

	// Constructor for thread implementation
	private AvgTempParallel(int l, int h) {
		start_i = l;
		end_i = h;
	}

	// Constructor for object implementation
	public AvgTempParallel(List<String> d, Boolean f) {
		data = d;
		fib = f;
	}

	// Divides the work among threads and computes the result
	public void ParallelAvg() throws Exception {
        
        
		int sp = Math.floorDiv(data.size(), 3);
   
        for(int i=0; i <10; i++) {
            Thread t1 = new Thread(new AvgTempParallel(0,sp));
            Thread t2 = new Thread(new AvgTempParallel((sp+1),(2*sp)));
            Thread t3 = new Thread(new AvgTempParallel(((2*sp)+1),(data.size()-1)));
	
  	      double start = System.currentTimeMillis();      	
          t1.start();
          t2.start();
          t3.start();

          t1.join();
          t2.join();
          t3.join();
        
          iter();
          double end = System.currentTimeMillis();
          t = end - start;
          runtime.add(t);
          tdata.clear();
          avg.clear();
        }
    //    printresult();
      
        
        
	}
}
