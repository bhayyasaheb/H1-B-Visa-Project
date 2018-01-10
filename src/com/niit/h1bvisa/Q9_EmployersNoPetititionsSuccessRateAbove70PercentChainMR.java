package com.niit.h1bvisa;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q9_EmployersNoPetititionsSuccessRateAbove70PercentChainMR {

	/*
	 * Mapper code
	 */
	public static class SuccessRateMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			String employer_name = record[2];
			String case_status = record[1];
			
			context.write(new Text(employer_name), new Text(case_status));
		}
		
	}
	
	/*
	 * Reducer code
	 */
	public static class SuccessRateReducer extends Reducer<Text, Text, DoubleWritable, Text>
	{		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			int totalApp= 0;
			int successApp = 0;
			
			for(Text val : values)
			{
				String  case_status = val.toString();
				
				if(case_status.equals("CERTIFIED") || case_status.equals("CERTIFIED-WITHDRAWN"))
				{
					successApp++;
					totalApp++;
				}
				else
				{
					totalApp++;
				}
			}
			
			double success_rate = (successApp*100)/totalApp;
			
			if(success_rate > 70 && totalApp >= 1000)
			{
				String employer_name = key.toString();
				
				String myValue = employer_name+"\t"+String.format("%d", totalApp);
				
				context.write(new DoubleWritable(success_rate), new Text(myValue));
			}
			
		}
	}
	
	/*
	 * second Mapper
	 */
	public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException 
		{
			String[] token = value.toString().split("\t");
			double success_rate = Double.parseDouble(token[0]);
			String employer_name = token[1];
			String totalApp = token[2];
			
			String myVal = employer_name+"\t"+totalApp;
			
			context.write(new DoubleWritable(success_rate), new Text(myVal));
		}
	}
	
	/*
	 * Second Reducer
	 */
	public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{

		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			for(Text val :values)
			{
				context.write(new Text(val), key);
			}
		}
		
	}
	
	/*
	 * Driver class or Configuration Code
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,"Employers along with the number of petitions who have the success rate more than 70%  in petitions. Using Chain MR");
		
		job.setJarByClass(Q9_EmployersNoPetititionsSuccessRateAbove70PercentChainMR.class);
		
		job.setMapperClass(SuccessRateMapper.class);
		job.setReducerClass(SuccessRateReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		Path outputpath1 = new Path("H1BFirstMapper");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputpath1);
		
		FileSystem.get(conf).delete(outputpath1, true);
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "Employers along with the number of petitions who have the success rate more than 70%  in petitions. Using Chain MR");
		
		job2.setJarByClass(Q9_EmployersNoPetititionsSuccessRateAbove70PercentChainMR.class);
		
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		
		job2.setSortComparatorClass(DecreasingComparator.class);
		
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, outputpath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		FileSystem.get(conf).delete(new Path(args[1]), true);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
