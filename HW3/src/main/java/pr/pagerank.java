package pr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;


public class pagerank {
/*
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			System.out.println("Input bz2 file required on command line.");
			System.exit(1);
		}

		BufferedReader reader = null;
		try {
			File inputFile = new File(args[0]);
			if (!inputFile.exists() || inputFile.isDirectory() || !inputFile.getName().endsWith(".bz2")) {
				System.out.println("Input File does not exist or not bz2 file: " + args[0]);
				System.exit(1);
			}
			BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
			reader = new BufferedReader(new InputStreamReader(inputStream));
			String line;
			while ((line = reader.readLine()) != null) {
				// Each line formatted as (Wiki-page-name:Wiki-page-html).
				System.out.println(line);
				int delimLoc = line.indexOf(':');
				String pageName = line.substring(0, delimLoc);
				String html = line.substring(delimLoc + 1);
				System.out.println(pageName);
				System.out.println(html);
			//	break;
			}		
		} catch (Exception e) {
				e.printStackTrace();
		} finally {
				try { reader.close(); } catch (IOException e) {}
		}
		

	}
*/
}
