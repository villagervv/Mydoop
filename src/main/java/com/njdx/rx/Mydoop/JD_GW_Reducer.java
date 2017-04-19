package com.njdx.rx.Mydoop;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class JD_GW_Reducer extends Reducer<Text, Text, Text, Text>{

	//根据map结果进行汇总
	public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		
		Set<String> l_value = new HashSet<String>();
		
		for(Text value : values){
			String jd_item = value.toString();
//			sb.append(jd_item);
//			sb.append(',');
			l_value.add(jd_item);
		}
		Iterator iter = l_value.iterator();
		while(iter.hasNext()){
			sb.append(iter.next());
			sb.append(',');
		}
		context.write(key, new Text(sb.toString()));
	}
}
