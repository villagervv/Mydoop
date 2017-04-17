package com.njdx.rx.Mydoop;


import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class JD_GW_Reducer extends Reducer<Text, Text, Text, Text>{

	//根据map结果进行汇总
	public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		
		for(Text value : values){
			String jd_item = value.toString();
			sb.append(jd_item);
			sb.append(',');
		}
		context.write(key, new Text(sb.toString()));
	}
}
