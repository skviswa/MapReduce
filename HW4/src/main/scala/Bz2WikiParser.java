
import java.io.IOException;
import java.io.Serializable;
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
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/** Decompresses bz2 file and parses Wikipages on each line. */
public class Bz2WikiParser implements Serializable{
	String pageName; 
	List<String> linkPageNames;
	
	public Bz2WikiParser(){
	}

	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}
	
// THIS IS THE SAME PARSER AS USED PREVIOUSLY WITH FOLLOWING MODIFICATIONS:
// THE main FUNCTION HAS BEEN RENAMED AS parser
// THE FUNCTION RETURNS NULL IF THERE IS NO MATCH OR IF THERE IS AN EXCEPTION
// OTHERWISE IT RETURNS A STRING 
// PAGENAME:ADJACENCYLIST
// I FILTER THE REPEATING PAGENAMES IN ADJACENCY LIST AND RETURN ONLY THE UNIQUE PAGENAMES
// IF IT IS A DANGLING NODE, I RETURN "~" AS THE ADJACENCY LIST, WHICH SERVES
// AS A MARKER FOR MY PAGE RANK PROGRAM	
	
//	public static Bz2WikiParser parser(String value) throws IOException {
	public String parser(String value) throws IOException {
		HashMap<String,Integer> uniquepages = new HashMap<String,Integer>();
		int j = value.indexOf(":");
		String name = new String(Arrays.copyOf(value.getBytes(), j), "latin1");
		if (!name.contains("~")) {
			String line = value.toString().replaceAll("&", "&amp;");

			try {
				
				// Configure parser.
				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				SAXParser saxParser = spf.newSAXParser();
				XMLReader xmlReader = saxParser.getXMLReader();
				// Parser fills this list with linked page names.
				List<String> linkPageNames = new LinkedList<String>();
				xmlReader.setContentHandler(new WikiParser(linkPageNames));

				// Each line formatted as (Wiki-page-name:Wiki-page-html).
				int delimLoc = line.indexOf(':');
				String pageName = line.substring(0, delimLoc);
				String html = line.substring(delimLoc + 1);
				Matcher matcher = namePattern.matcher(pageName);
				if (!matcher.find()) {
					// Skip this html file, name contains (~).
					return null;
				}

				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					xmlReader.parse(new InputSource(new StringReader(html)));
				} catch (Exception e) {
					// Discard ill-formatted pages.
					return null;
				}

				if (linkPageNames.isEmpty()) {
					return (pageName + ":~");
					
				} 
				
				for(int z = 0; z < linkPageNames.size(); z++) {
					if(!uniquepages.containsKey(linkPageNames.get(z))) {
						uniquepages.put(linkPageNames.get(z), 1);
					}
							
				}
				StringBuilder res =  new StringBuilder();
				res.append(pageName + ":");
		    	for (String s : uniquepages.keySet()) {
		    		res.append(s + ",");
		    	}	
		    	res.setLength(Math.max(res.length() - 1, 0));
		    	return res.toString();
				//return (pageName +":"+linkPageNames.toString());
				

			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} 
		}
		return null;
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
