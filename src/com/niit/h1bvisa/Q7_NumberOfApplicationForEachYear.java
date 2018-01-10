package com.niit.h1bvisa;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q7_NumberOfApplicationForEachYear {

	public static class YearMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String mySearchText = context.getConfiguration().get("myText");
			
			String[] record = value.toString().split("\t");
			
			String year = record[7];
			
			if(mySearchText.equals("ALL"))
			{
				context.write(new Text(year), new IntWritable(1));
			}
			else
			{
				if(year.equals(mySearchText))
				{
					context.write(new Text(year), new IntWritable(1));
				}
			}
		}
		
	}
	
	public static class YearReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
		{
			int sum = 0;
			for(IntWritable val :values)
			{
				sum += val.get();
			}
			
			context.write(new Text(key), new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		if(args.length > 2)
		{
			conf.set("myText", args[2]);
		}
		else
		{
			System.out.println("Number of arguments should be 3");
			System.exit(0);
		}
		
		Job job = Job.getInstance(conf,"Number of Applications for each Year");
		
		job.setJarByClass(Q7_NumberOfApplicationForEachYear.class);
		
		job.setMapperClass(YearMapper.class);
		job.setReducerClass(YearReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
