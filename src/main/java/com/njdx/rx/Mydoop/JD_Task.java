package com.njdx.rx.Mydoop;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JD_Task {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String hdfsInput = args[0]; //输入路径
		String hdfsOutput = args[1]; //输出路径
		
		Configuration conf = new Configuration();
		
		conf.set("mapreduce.job.queuename", "rtbqueue");
	    conf.set("mapreduce.output.textoutputformat.separator", "^"); //key, value之间的分隔符
	    
	    Job job = Job.getInstance(conf, "ML_WX"); //任务名
	    
		//控制map和reduce的输出类型<K,V>
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
		//运行的map和reduce类
	    //job.setMapperClass(My_Acc_Mapper.class); //Map类

	    job.setMapperClass(JD_GW_Mapper.class); //Map类
	    job.setReducerClass(JD_GW_Reducer.class); //Reduce类
	    
	    job.setNumReduceTasks(10);
		//setInputFormatClass定义的InputFormat将输入的数据集分割成小数据
	    job.setInputFormatClass(TextInputFormat.class);
		//输出数据格式
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
		//输入输出路径 配置 
	    FileInputFormat.setInputPaths(job, hdfsInput);
	    FileOutputFormat.setOutputPath(job, new Path(hdfsOutput));
	    
	    FileOutputFormat.setCompressOutput(job, false);
	   
	    job.setJarByClass(JD_Task.class); //入口类
	    
	    job.waitForCompletion(true);        
	}
	
}
