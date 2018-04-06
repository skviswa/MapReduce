package preprocess;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

// This class runs a pre-processing job.
// It reads the bz2 input, uses the SAX parser to obtain pagename and linknames
// I then emit each pagename in the adjacency list
// I do this because in case records are repeated, then this gives me a better way to consolidate the records
// I have borrowed Justin's code and suggestion to use latin1 encoding for the pagename
// I have also incorporated YuQing's suggested fix for the & error 


public class FirstMapper {
	public static class ProcessMapper extends Mapper<Object, Text, Text, Text>{

	private static Pattern namePattern;
	private static Pattern linkPattern;

	protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException{

		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");

	}
	
	//public HashMap<String,Integer> pages = new HashMap<String,Integer>(); // Hashmap to keep track of unique local pagenames
	   @Override
	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		HashMap<String,Integer> uniquepages = new HashMap<String,Integer>();
		int j = value.find(":");
		String name = new String(Arrays.copyOf(value.getBytes(), j), "latin1");
		if(!name.contains("~")) {
		String line = value.toString().replaceAll("&", "&amp;");
		try {

		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		// Parser fills this list with linked page names.
		List<String> linkPageNames = new LinkedList<>();
		xmlReader.setContentHandler(new WikiParser(linkPageNames));

		//	String line = line1.replaceAll("[]", "");
			// Each line formatted as (Wiki-page-name:Wiki-page-html).
			int delimLoc = line.indexOf(':');
			String pageName = name;//line.substring(0, delimLoc);
			String html = line.substring(delimLoc + 1);
			Matcher matcher = namePattern.matcher(pageName);
			if (!matcher.find()) {
				// Skip this html file, name contains (~).
				
			}
			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				// Discard ill-formatted pages.
				
			}
		//	pageName = pageName.replaceAll("\\t+","").replaceAll("\\s+","");
			if (linkPageNames.isEmpty()) {
				context.write(new Text(pageName), new Text(""));
			} 
		//	uniquepages.put
			for(int z = 0; z < linkPageNames.size(); z++) {
				String temp = linkPageNames.get(z);
//				temp = temp.replaceAll("\\[", "").replaceAll("\\]", "");
			//	temp = temp.replaceAll("\\t+","").replaceAll("\\s+","");
				if(!uniquepages.containsKey(temp)) {
					uniquepages.put(temp, 1);
					
				}
						
			}
			StringBuilder res =  new StringBuilder();
	    	for (String s : uniquepages.keySet()) {
	    		context.write(new Text(s), new Text(""));
	    		res.append(s + "~");
	    	}	
	    	res.setLength(Math.max(res.length() - 1, 0));
	    	context.write(new Text(pageName), new Text(res.toString()));	// emit each pagename in adjacency list
				
		} catch (Exception e) {
			e.printStackTrace();
		}
		}	
	   }	  
	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}
     

	}
	

}
