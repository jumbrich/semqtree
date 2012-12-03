package edu.kit.aifb.semqtree.vldbpeople;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import junit.framework.TestCase;


public class StreamTest extends TestCase {
	public void testStream() throws Exception {
		URL u = new URL("http://sw.deri.org/~aharth/cgi-bin/tweets");
		
		HttpURLConnection con = (HttpURLConnection)u.openConnection();
		con.connect();
		
		InputStream is = con.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		
		String line = null;
		
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}
	}
}
