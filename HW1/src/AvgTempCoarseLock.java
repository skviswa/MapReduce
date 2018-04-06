import java.util.*;
import java.lang.Runnable;
import java.lang.Math;

public class AvgTempCoarseLock implements Runnable {
	private static List<String> data = new ArrayList<>();
	private static volatile HashMap<String, int[]> tdata = new HashMap<String, int[]>();
    
	// Contains final avg result per station id
	private static HashMap<String, Float> avg = new HashMap<String, Float>();

	private int start_i;
	private int end_i;
	
	static Boolean fib;
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
			Fibonacci ft = new Fibonacci();
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
	
		        synchronized(tdata) {
				if(tdata.containsKey(split[j])) {
					
            	  temp = tdata.get(split[j]);
            	  temp[0] = temp[0] + Integer.parseInt(split[j+3]);
            	  temp[1] = temp[1] + 1;
            	  if(fib)
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
		 }
            
			i++;
		}
		
	}
	
	@Override	
	public void run() {
	
		AccData();
	}

	private AvgTempCoarseLock(int l, int h) {
		start_i = l;
		end_i = h;
	}

	public AvgTempCoarseLock(List<String> d, Boolean f) {
		data = d;
		fib = f;
	}

	public void ParallelAvg() throws Exception {
        
        int sp = Math.floorDiv(data.size(), 3);
   
        for(int i=0; i <10; i++) {
            Thread t1 = new Thread(new AvgTempCoarseLock(0,sp));
            Thread t2 = new Thread(new AvgTempCoarseLock((sp+1),(2*sp)));
            Thread t3 = new Thread(new AvgTempCoarseLock(((2*sp)+1),(data.size()-1)));

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
        
        
	}

}
