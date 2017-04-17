package com.njdx.rx.Mydoop;

import java.io.IOException;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import sun.misc.BASE64Decoder;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import sun.misc.BASE64Decoder;

public class JD_GW_Mapper extends Mapper<Object, Text, Text, Text>{

	private String line;
	
	BASE64Decoder decoder = new BASE64Decoder();
	public void run(Context context) {
        try {
            setup(context);
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
            cleanup(context);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
	}
	
    protected void setup(Context context) throws IOException, InterruptedException {
		//初始化在此处完成
        super.setup(context);
    }
    
    public void map(Object key, Text value, Context context){   	
		try{
			line = value.toString(); //DPI记录
//	    	String target_host = "item.jd.com";
			String target_referer = "http://mp.weixin.qq.com/s?__biz";  
			String target_video = "http://mp.weixin.qq.com/mp/videoplayer?";
            String[] temp = line.split("\\|", -1);
			if(temp.length == 12){
				String uid = temp[0]; //用户账号
	            byte[] c = decoder.decodeBuffer(temp[7]);
	    		String url = new String(c); //URL字段
	    		byte[] b = decoder.decodeBuffer(temp[6]);
	    		String host = new String(b); //HOST字段
	    		
	    		byte[] r = decoder.decodeBuffer(temp[8]);
	    		String referer = new String(r);//Referrer字段
	    		
	    		if(referer.contains(target_referer)||referer.contains(target_video)){/*
					int startIndex = url.lastIndexOf("/");
					int endIndex = url.indexOf(".html");
					if(startIndex != -1 && endIndex != -1){
						String result = url.substring(startIndex+1, endIndex);
		    			if(result != null){
		    				context.write(new Text(uid), new Text(result));
		    			}
					}*/
	    			String result = referer;
	    			if(result != null){
	    				context.write(new Text(uid), new Text(result));
	    			}
	    		}
			}    
		//需要根据具体情况捕获异常，应避免以下写法
		} catch(Exception e){
			e.printStackTrace();
			return;
		}
    }
}