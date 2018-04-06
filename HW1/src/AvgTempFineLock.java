import java.util.*;
import java.lang.Runnable;
import java.lang.Math;

public class AvgTempFineLock implements Runnable {
	private static List<String> data = new ArrayList<>();
	private static volatile HashMap<String, int[]> tdata = new HashMap<String, int[]>();
    
	// Contains final avg result per station id
	private HashMap<String, Float> avg = new HashMap<String, Float>();

	private int start_i;
	private int end_i;
	private static volatile String key; // This is the variable used to lock the HashMap updation process
	
	static Boolean fib; // To enable Fibonacci(17)
	// For calculating time per run
    double t;

    // To hold and compute the average
	 List<Double> runtime = new ArrayList<>();
	
	public void printresult() {
	    Iterator<Map.Entry<String, Float>> i = avg.entrySet().iterator();
	    System.out.println("Station ID : Avg. Temp.\n");
	    while (i.hasNext()) {
           Map.Entry<String, Float> entry = i.next();
	       System.out.println(entry.getKey() + " : " + entry.getValue());
	    }
	}
	
	public void iter() {
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
	
	// Function to convert input data in to our accumulated data structure
		private void AccData() {
		int i = start_i;
		String s = data.get(start_i);
		int j = 0;
   //     Boolean f; 
		while(i <= end_i) {
			s = data.get(i);
			String[] split = s.split(",");
			if(split[j+2].equals("TMAX")) {

       		    if(split[j+3].isEmpty()) {
       		        i++;
       		    	continue;
       		    }
		        int temp[] = new int[2];

                 Boolean f;
                 key = split[j]; // key is used to lock the process
                	f = tdata.containsKey(key); // This is most important as this decides the if else clause
                 }
				if(f) {
                  synchronized(key) {
	
				  temp = tdata.get(key);
				  temp[0] = temp[0] + Integer.parseInt(split[j+3]);
            	  temp[1] = temp[1] + 1;
   				//synchronized(key) {  	
             	  tdata.put(key, temp); // enough if we lock during put stage.
				 }
   			    }
				else {
					 synchronized(tdata) {
	            	  temp[0] = Integer.parseInt(split[j+3]);
	            	  temp[1] = 1;
	               	  tdata.put(key, temp);
					 }
				}
		        }
			i++;
		}
		
	}
	
	@Override	
	public void run() {
	
		AccData();
	}
     
	// Generic constructor for the thread tasks
	private AvgTempFineLock(int l, int h) {
		start_i = l;
		end_i = h;
	//	key = null;
	}

	// generic constructor for the object tasks
	public AvgTempFineLock(List<String> d, Boolean f) {
		data = d;
		fib = f;
		key = null;
	}

	public void ParallelAvg() throws Exception {
        
		// 3 threads are initiated, so division by 3
        int sp = Math.floorDiv(data.size(), 3);
        // repeat task 10 times
        for(int i=0; i <10; i++) {
            Thread t1 = new Thread(new AvgTempFineLock(0,sp));
            Thread t2 = new Thread(new AvgTempFineLock((sp+1),(2*sp)));
            Thread t3 = new Thread(new AvgTempFineLock(((2*sp)+1),(data.size()-1)));

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
            runtime.add(t); // each time keep track of the runtime
            tdata.clear();
            avg.clear();
          }
        
        
	}

}
