package com.github.sharplog.samples.getbingpic;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BingPicGetter {
	public static void main(String[] args){
	    HttpURLConnection connection = null;
	    String picPath = "E:/Pictures/";
	    
	    try {
	    	String urlstr = "http://cn.bing.com";
	        URL url = new URL(urlstr);
	        connection = (HttpURLConnection)url.openConnection();
	        connection.connect();
	        InputStream urlStream = connection.getInputStream();
	        BufferedReader bf = new BufferedReader(new InputStreamReader(urlStream));
	        StringBuffer sb = new StringBuffer();
	        
	        String s;
	        while( null != (s = bf.readLine())){
	        	sb.append(s);
	        }
	        
	        String furl = null;
	        String fname = null;
	        Pattern p = Pattern.compile("url:'(http:.*1920x1080.jpg)',id:");
	        Matcher m = p.matcher(sb);

	        if( m.find() ){
	        	furl = m.group(1);
		        Pattern pp = Pattern.compile("/([A-Za-z]+)_");
		        Matcher mm = pp.matcher(furl);
		        if( mm.find() ){
		        	fname = mm.group(1);
		        };
	        }
	        
	        if( null != furl && null != fname ){
	        	
	        }
	        
	        url = new URL(furl);
	        connection = (HttpURLConnection)url.openConnection();
	        connection.connect();
	        urlStream = connection.getInputStream();
	        DataOutputStream f = new DataOutputStream(new FileOutputStream(new File(picPath + fname + ".jpg")));
	        byte[] b = new byte[1024];
	        while( true){
	        	int l = urlStream.read(b);
	        	if( l == -1 ) break;
	        	f.write(b, 0, l);
	        }
	        f.flush();
	        f.close();
	        
	        System.out.println("Get picture: " + fname);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }finally{
			if(null != connection){
				connection.disconnect();
			}
	    }
	}

}
