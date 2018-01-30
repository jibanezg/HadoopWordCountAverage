package com.mum.edu;
        
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCountAverage {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String u = getUID(value.toString());
        String r =getRSize(value.toString());
        
        context.write(new Text(u), new IntWritable(Integer.valueOf(r)));
    }
    
    private static String getUID(String line){
    	String[] values = line.split(" ");
    	return values[0];
    }
    
    private static String getRSize(String line){
    	String[] values = line.split(" ");
    	return values[values.length-1];
    }
    
 } 
 
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
    	int sum =0,cnt = 0;
		 for(IntWritable r: values){
			 sum += r.get();
			 cnt ++;
		 }
		 context.write(key, new IntWritable(sum/cnt));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcountaverage");
        job.setJarByClass(com.mum.edu.WordCountAverage.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
