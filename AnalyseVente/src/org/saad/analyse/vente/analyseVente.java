package org.saad.analyse.vente;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.file.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class analyseVente{

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
			sum += val.get();
			
			}
			if (sum<0)
				sum=-sum;
			result.set(sum);
			//System.out.println("\nLet's put a smile on that face"+sum);
			context.write(key, result);
		}
	}
	
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
		public void map(LongWritable key,Text value,Context context) 
				throws IOException,InterruptedException
		{
			
			String Line = value.toString();
			StringTokenizer token = new StringTokenizer(Line,",");
				String s1="0";
				String s = token.nextToken();
				//System.out.println("\nWhy so serious:-"+s);
				value.set(s);
				int count=0;
				while(token.hasMoreTokens())
				{
					s1 = token.nextToken();
					count++;
				}
				double number = Double.parseDouble(s1);
				result = (int) number;
				//System.out.println("\nSysout "+count);
			context.write(value, new IntWritable(result));//1
			
		}
		
		
	}
	
	
	public static class Map1 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
	public void map(LongWritable key,Text value,Context context) 
			throws IOException,InterruptedException
	{
		
		String Line = value.toString();
		StringTokenizer token = new StringTokenizer(Line,",");
			String s1="0";
			String s = token.nextToken();
			//System.out.println("\nWhy so serious:-"+s);
			value.set(s);
			int count=0;
			while(token.hasMoreTokens())
			{
				
				s1 = token.nextToken();
				if (count==8)
					break;
				count++;
			}
			double number = Double.parseDouble(s1);
			result = (int) number;
			//System.out.println("\nSysout "+count);
		context.write(value, new IntWritable(result));//1
		
	}
	
	
}
	public static class Map2 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
	public void map(LongWritable key,Text value,Context context) 
			throws IOException,InterruptedException
	{
		
		String Line = value.toString();
		StringTokenizer token = new StringTokenizer(Line,",");
			String s1="0";
			String s = token.nextToken();
			//System.out.println("\nWhy so serious:-"+s);
			value.set(s);
			int count=0;
			while(token.hasMoreTokens())
			{
				
				s1 = token.nextToken();
				if (count==10)
					break;
				count++;
			}
			double number = Double.parseDouble(s1);
			result = (int) number;
			//System.out.println("\nSysout "+count);
		context.write(value, new IntWritable(result));//1
		
	}
	
	
}
	public static class Map3 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
		public void map(LongWritable key,Text value,Context context) 
				throws IOException,InterruptedException
		{
			
			String Line = value.toString();
			StringTokenizer token = new StringTokenizer(Line,",");
				String s1="0";
				String s = token.nextToken();
				s=token.nextToken();
				System.out.println(s);
				//System.out.println("\nWhy so serious:-"+s);
				value.set(s);
				int count=0;
				while(token.hasMoreTokens())
				{
					s1 = token.nextToken();
					count++;
				}
				double number = Double.parseDouble(s1);
				result = (int) number;
				//System.out.println("\nSysout "+count);
			context.write(value, new IntWritable(result));//1
			
		}
		
		
	}
	
	
	public static class Map4 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
	public void map(LongWritable key,Text value,Context context) 
			throws IOException,InterruptedException
	{
		
		String Line = value.toString();
		StringTokenizer token = new StringTokenizer(Line,",");
			String s1="0";
			String s = token.nextToken();
			s=token.nextToken();
			System.out.println(s);
			//System.out.println("\nWhy so serious:-"+s);
			value.set(s);
			int count=0;
			while(token.hasMoreTokens())
			{
				
				s1 = token.nextToken();
				if (count==8)
					break;
				count++;
			}
			double number = Double.parseDouble(s1);
			result = (int) number;
			//System.out.println("\nSysout "+count);
		context.write(value, new IntWritable(result));//1
		
	}
	
	
}
	public static class Map5 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
	public void map(LongWritable key,Text value,Context context) 
			throws IOException,InterruptedException
	{
		
		String Line = value.toString();
		StringTokenizer token = new StringTokenizer(Line,",");
			String s1="0";
			String s = token.nextToken();
			s=token.nextToken();
			System.out.println(s);
			//System.out.println("\nWhy so serious:-"+s);
			value.set(s);
			int count=0;
			while(token.hasMoreTokens())
			{
				
				s1 = token.nextToken();
				if (count==10)
					break;
				count++;
			}
			double number = Double.parseDouble(s1);
			result = (int) number;
			//System.out.println("\nSysout "+count);
		context.write(value, new IntWritable(result));//1
		
	}
	}
	
	public static class Map6 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
		public void map(LongWritable key,Text value,Context context) 
				throws IOException,InterruptedException
		{
			
			String Line = value.toString();
			StringTokenizer token = new StringTokenizer(Line,",");
				String s1="0";
				String s = token.nextToken();
				s=token.nextToken();
				s=token.nextToken();
				System.out.println(s);
				//System.out.println("\nWhy so serious:-"+s);
				value.set(s);
				int count=0;
				while(token.hasMoreTokens())
				{
					s1 = token.nextToken();
					count++;
				}
				double number = Double.parseDouble(s1);
				result = (int) number;
				//System.out.println("\nSysout "+count);
			context.write(value, new IntWritable(result));//1
			
		}
		
		
	}
	
	
	public static class Map7 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
	public void map(LongWritable key,Text value,Context context) 
			throws IOException,InterruptedException
	{
		
		String Line = value.toString();
		StringTokenizer token = new StringTokenizer(Line,",");
			String s1="0";
			String s = token.nextToken();
			s=token.nextToken();
			s=token.nextToken();
			System.out.println(s);
			//System.out.println("\nWhy so serious:-"+s);
			value.set(s);
			int count=0;
			while(token.hasMoreTokens())
			{
				
				s1 = token.nextToken();
				if (count==8)
					break;
				count++;
			}
			double number = Double.parseDouble(s1);
			result = (int) number;
			//System.out.println("\nSysout "+count);
		context.write(value, new IntWritable(result));//1
		
	}
	
	
}
	public static class Map8 extends Mapper<LongWritable,Text,Text,IntWritable>{
		int result;
	public void map(LongWritable key,Text value,Context context) 
			throws IOException,InterruptedException
	{
		
		String Line = value.toString();
		StringTokenizer token = new StringTokenizer(Line,",");
			String s1="0";
			String s = token.nextToken();
			s=token.nextToken();
			s=token.nextToken();
			System.out.println(s);
			//System.out.println("\nWhy so serious:-"+s);
			value.set(s);
			int count=0;
			while(token.hasMoreTokens())
			{
				
				s1 = token.nextToken();
				if (count==10)
					break;
				count++;
			}
			double number = Double.parseDouble(s1);
			result = (int) number;
			//System.out.println("\nSysout "+count);
		context.write(value, new IntWritable(result));//1
		
	}
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//-------------------------------------------------------------------------
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		FileSystem hdfs =FileSystem.get(conf);
		Path hdfsFilePath=new Path("/user/neeraj/o1");
		Path localFilePath=new Path("/home/neeraj/Desktop/project/output");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//-----------------------------------------------------------------------
		conf = new Configuration();
	    otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map1.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output1");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//----------------------------------------------------------------------
		conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map2.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output2");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//-----------------------------------------------------------------------
		conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map3.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output3");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//-----------------------------------------------------------------------
		conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map4.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output4");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//-----------------------------------------------------------------------
		conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map5.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output5");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//------------------------------------------------------------------------
		conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map6.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output6");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//-----------------------------------------------------------------------
		conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map7.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output7");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//------------------------------------------------------------------------
		conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
		System.err.println("Usage: wordcount <in> [<in>...] <out>");
		System.exit(2);
		}
		
		job = Job.getInstance(conf, "project");
		job.setJarByClass(analyseVente.class);
		job.setMapperClass(Map8.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);		
		hdfs =FileSystem.get(conf);
		hdfsFilePath=new Path("/user/neeraj/o1");
		localFilePath=new Path("/home/neeraj/Desktop/project/output8");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		hdfs.delete(hdfsFilePath, true);
		//-----------------------------------------------------------------------
        //----------------------------------------------------------------------------
		
	}
}