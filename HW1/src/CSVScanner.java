import java.util.*;
import java.io.*;

// A class to scan the information of a document
public class CSVScanner {
    
	// Defines the List of String data structure
	public List<String> data = new ArrayList<>();
	
	// Returns the data structure
	public List<String> getData() {
		return data;
	}
	
	// Constructor which initializes the data structure
	public CSVScanner(String arg) throws IOException {

		//Operator To read the data in the file
    	BufferedReader in = new BufferedReader(new FileReader(arg));	 
	    String str;
	
    	while((str = in.readLine()) != null) {
	     	data.add(str);
        }
	
	    in.close();
	}

}
