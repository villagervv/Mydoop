package com.njdx.rx.Mydoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import sun.misc.BASE64Decoder;


public class My_Acc_Mapper extends Mapper<Object, Text, Text, Text>{

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
            String[] temp = line.split("\\|", -1);
			if(temp.length == 12){
				String uid = temp[0]; //用户账号
				byte[] a=decoder.decodeBuffer(temp[9]);
				String ua = new String (a);
				
	            byte[] c = decoder.decodeBuffer(temp[7]);
	    		String url = new String(c); //URL字段
	    		byte[] b = decoder.decodeBuffer(temp[6]);
	    		String host = new String(b); //HOST字段
				//获取JD商品页面URL中的商品id	    		
				
	    		if(uid.toLowerCase().contains("107418932")){
	    				String result = url+"^^";
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
