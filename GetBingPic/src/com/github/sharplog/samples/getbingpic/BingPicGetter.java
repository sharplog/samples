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
        final String key_path = "--path=";
        final String key_parttern = "--parttern=";
        final String key_help = "--help";
        
    	HttpURLConnection connection = null;
        String picPath = "E:/Pictures/background/";
        String partternStr = "url: \"(\\/.+?1920x1080.jpg)\",";
        
        for (int i = 0; i < args.length; i++) {
        	if( args[i].equals(key_help) ){
        		printUsage();
        		continue;
        	}
        	else if(args[i].startsWith(key_path)){
        		picPath = args[i].substring(key_path.length());
        		continue;
        	}
        	else if(args[i].startsWith(key_parttern)){
        		partternStr = args[i].substring(key_parttern.length());
        		continue;
        	}
        	else{
        		System.out.println("Don't known option: " + args[i]);
        		System.out.println();
        		printUsage();
        		return;
        	}
        }
        
        try {
            String urlstr = "http://cn.bing.com";
            //Pattern p = Pattern.compile("url:'(http:.*1920x1080.jpg)',id:");
            //Pattern p = Pattern.compile("url: \"(http:.+?1920x1080.jpg)\",");
            Pattern p = Pattern.compile(partternStr);

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
            Matcher m = p.matcher(sb);
            if( m.find() ){
                furl = m.group(1);
                furl = urlstr + furl.replaceAll("\\\\", "");
            }

            String fname = null;
            if( null != furl ){
	            Pattern pp = Pattern.compile("/([A-Za-z]+)_");
	            Matcher mm = pp.matcher(furl);
	            if( mm.find() ){
	            	fname = mm.group(1);
	            };
            }
            
            if( null != fname ){
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
            }
            else{
            	System.out.println("Can't get picture file.");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(null != connection){
                connection.disconnect();
            }
        }
    }

	private static void printUsage() {
		System.out.println("Usage: java -jar binggetter.jar [option]...");
		System.out.println();
		System.out.println("Get background picture from cn.bing.com.");
		System.out.println();
		System.out.println("Options:");
		System.out.println("\t--help           print this message.");
		System.out.println("\t--path=PATH      save picture to PATH.");
		System.out.println("\t--parttern=EXPR  use regular expression EXPR to parse picture url.");
	}

}
