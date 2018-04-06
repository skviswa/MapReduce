import java.util.*;

public class AvgTemp {

	
	// Data Structure to hold list of sum and count per station id
	// Key is station id and the list of temperature is the value
	public HashMap<String, int[]> tdata = new HashMap<String, int[]>();
    
	// Contains final avg result per station id
	public HashMap<String, Float> avg = new HashMap<String, Float>();
	
    public List<String> data = new ArrayList<>();
	
    Boolean fib;
    // To compute time per run
    double t;

    // To hold and compute the average
	 List<Double> runtime = new ArrayList<>();
    
    // Function to find average TMax per station
	public void AvgSeq(List<String> data) {
		AccData(data);
	    iter();
	
	}
	
	// Prints the final result
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
            temp = entry.getValue();
            average = (float)temp[0]/temp[1];
            avg.put(entry.getKey(), average);
	    }
	}
	
	// Function to convert input data in to our accumulated data structure
	public void AccData(List<String> data) {
		
		Fibonacci ft = new Fibonacci();
		int i = 0;
		int len = data.size();
		String s = data.get(0);
		int j = 0;

		
		while(i < len) {
			s = data.get(i);
			String[] split = s.split(",");
			if(split[j+2].equals("TMAX")) { // If the line contains TMAX
       		    if(split[j+3].isEmpty()) { // If there is no data, skip
       		        i++; 
       		    	continue;
       		    }
                 
		        int temp[] = new int[2];
				if(tdata.containsKey(split[j])) { // Check if HashMap has a partial data already for the key
            	  temp = tdata.get(split[j]);
            	  temp[0] = temp[0] + Integer.parseInt(split[j+3]);
            	  temp[1] = temp[1] + 1;
            	  if(fib)
            		  ft.f();
            	  tdata.put(split[j], temp);
   			    }
				else {
	            	  temp[0] = Integer.parseInt(split[j+3]); // Else enter the information first time
	            	  temp[1] = 1;
	            	  if(fib)
	            		  ft.f();
     	  
	            	  tdata.put(split[j], temp);
				}
			}
            
			i++;
	   }
		
	}
	
	// Checks if average produced is equal to another HashMap
public boolean equals(HashMap<String, Float> a) {
	if(avg.equals(a))
		return true;
	else
		return false;
}
	
// Constructor that calls average function according to specified method	
 public AvgTemp(List<String> data, Boolean f) {	
 
    this.data = data; 
    fib = f;
 }	 
 
 public void RunSeq() {
	 
	 for (int i=0; i<10; i++) {
	      double start = System.currentTimeMillis();
  		  AvgSeq(data);
          double end = System.currentTimeMillis();
          t = end - start;
          runtime.add(t);
          tdata.clear();
          avg.clear();
    }
  	 
 }

public AvgTemp() {
	// TODO Auto-generated constructor stub
}
 
}
