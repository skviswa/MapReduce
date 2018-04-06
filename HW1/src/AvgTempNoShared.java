import java.lang.Runnable;
import java.util.*;
import java.lang.Math;


public class AvgTempNoShared implements Runnable {

	// Data structure which is used to collect the per thread accumulation data structures
	public static volatile List<HashMap<String, int[]>> result = new ArrayList<HashMap<String, int[]>>();
	private static List<String> data = new ArrayList<String>();
	
	// Maintains the indices to split work to threads
	private int start_i;
	private int end_i;
	
    // for fibonacci(17) 
	static Boolean fib;
	// For calculating time per run
    double t;

    // To hold and compute the average
	 List<Double> runtime = new ArrayList<>();

	
	public AvgTempNoShared(List<String> d, Boolean f) {
	   data = d;
	   fib = f;
	}
	
	// Constructor for threads to initialize splitting data
	public AvgTempNoShared(int l, int h) {
		   start_i = l;
		   end_i = h;
		}

	@Override
	public void run() {
		AccData();
	}
	
	// Parses each line and finds the TMax values keeping partial sums and count
	private void AccData() {
	Fibonacci ft = new Fibonacci();	
	HashMap<String, int[]> tdata = new HashMap<String, int[]>();	// temporary per thread hashmap defined
	int i = start_i;
	String s = data.get(start_i);
	int j = 0;

	while(i <= end_i) {
		s = data.get(i);
		String[] split = s.split(",");
		if(split[j+2].equals("TMAX")) {

   		    if(split[j+3].isEmpty()) {
   		        i++;
   		    	continue;
   		    }
             
	        int temp[] = new int[2];

			if(tdata.containsKey(split[j])) {
				
        	  temp = tdata.get(split[j]);
        	  temp[0] = temp[0] + Integer.parseInt(split[j+3]);
        	  temp[1] = temp[1] + 1;
        	  if(fib)                       // for Fibonacci(17)
        		  ft.f();
        	  tdata.put(split[j], temp);
			    }
			else {
            	  temp[0] = Integer.parseInt(split[j+3]);
            	  temp[1] = 1;
            	  if(fib)
            		  ft.f();
               	  tdata.put(split[j], temp);
			}

	 }
        
		i++;
	}
	synchronized(result) {
	result.add(tdata);  // need to add the hashmaps without race problems
	}
}

	// Prints the final result
	public void printresult(HashMap<String, Float> avg) {
	    Iterator<Map.Entry<String, Float>> i = avg.entrySet().iterator();
	    System.out.println("Station ID : Avg. Temp.\n");
	    while (i.hasNext()) {
           Map.Entry<String, Float> entry = i.next();
	       System.out.println(entry.getKey() + " : " + entry.getValue());
	    }
	}
	
	// Computes average given the accumulated data structure
	public void iter(HashMap<String, int[]> tdata) {
		HashMap<String, Float> avg = new HashMap<String, Float>();
        int temp[] = new int[2];
        float average;
	    Iterator<Map.Entry<String, int[]>> i = tdata.entrySet().iterator();
	    while (i.hasNext()) {
	        Map.Entry<String, int[]> entry = i.next();
            temp = entry.getValue();
            average = (float)temp[0]/temp[1];
            avg.put(entry.getKey(), average);
	    }
	  
	}
	
	// Combines the result from each threads.
	private HashMap<String, int[]> accumresult() {
		HashMap<String, int[]> tdata = new HashMap<String, int []>();
		tdata = result.get(0); // First element in list is taken as is
		int j;
		for(j=1;j<result.size();j++) {
		    Iterator<Map.Entry<String, int[]>> i = (result.get(j)).entrySet().iterator();
		    while (i.hasNext()) {
	           Map.Entry<String, int[]> entry = i.next(); // For subsequent elements, add by inspecting key
	           if(tdata.containsKey(entry.getKey())) {
	               int temp[] = new int[2];
	         	  temp = tdata.get(entry.getKey());
	        	  temp[0] = temp[0] + entry.getValue()[0];
	        	  temp[1] = temp[1] + entry.getValue()[1];
	        	  tdata.put(entry.getKey(), temp);

	           }
	           else 
	        	   tdata.put(entry.getKey(), entry.getValue());
		    }
		}
		return tdata;
	}
	
	// Divides the work among threads and computes the result
	public void ParallelNoShare() throws Exception {
		
		int sp = Math.floorDiv(data.size(), 3);

        for(int i=0; i <10; i++) {
            Thread t1 = new Thread(new AvgTempNoShared(0,sp));
            Thread t2 = new Thread(new AvgTempNoShared((sp+1),(2*sp)));
            Thread t3 = new Thread(new AvgTempNoShared(((2*sp)+1),(data.size()-1)));
        	
    		HashMap<String, int[]> ti = new HashMap<String, int[]>();
     	    double start = System.currentTimeMillis();      	
            t1.start();
            t2.start();
            t3.start();

            t1.join();
            t2.join();
            t3.join();
        
            ti = accumresult();
            iter(ti);      
            double end = System.currentTimeMillis();
            t = end - start;
            runtime.add(t);
            result.clear();
                   
	    }
	}
}
