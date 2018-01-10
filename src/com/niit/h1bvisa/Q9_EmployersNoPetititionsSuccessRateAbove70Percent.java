package com.niit.h1bvisa;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q9_EmployersNoPetititionsSuccessRateAbove70Percent {

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
	public static class SuccessRateReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Double,Text> map = new TreeMap<Double, Text>();
		
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
				
				String myValue = employer_name+","+String.format("%d", totalApp)+","+String.format("%f", success_rate);
				
				map.put(success_rate, new Text(myValue));
			}
			
		}

		@Override
		protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)throws IOException, InterruptedException 
		{
			for(Text top : map.descendingMap().values())
			{
				context.write(NullWritable.get(), top);
			}
		}	
	}
	
	/*
	 * Driver class or Configuration code
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Employers along with the number of petitions who have the success rate more than 70%  in petitions.");
		
		job.setJarByClass(Q9_EmployersNoPetititionsSuccessRateAbove70Percent.class);
		
		job.setMapperClass(SuccessRateMapper.class);
		job.setReducerClass(SuccessRateReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
