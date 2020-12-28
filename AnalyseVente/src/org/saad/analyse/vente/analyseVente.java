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
	
}