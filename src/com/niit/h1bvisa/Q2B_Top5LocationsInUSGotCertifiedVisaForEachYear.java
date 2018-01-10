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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2B_Top5LocationsInUSGotCertifiedVisaForEachYear {

	public static class Top5LocationsMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String mySearchText = context.getConfiguration().get("myText");
			
			String[] record = value.toString().split("\t");
			String year = record[7];
			String location = record[8];
			String case_status = record[1];
			
			if(mySearchText.equals("ALL"))
			{
				if(case_status.equals("CERTIFIED"))
				{
					context.write(new Text(location), new Text(year));
				}
			}
			else
			{
				if(case_status.equals("CERTIFIED") && year.equals(mySearchText))
				{
					context.write(new Text(location), new Text(year));
				}
			}
		}
	}
	
	public static class YearPartitioner extends Partitioner<Text, Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			
			String year = value.toString();
			
			if(year.equals("2011"))
			{
				return 0 % numReduceTasks;
			}
			else if(year.equals("2012"))
			{
				return 1 % numReduceTasks;
			}
			else if(year.equals("2013"))
			{
				return 2 % numReduceTasks;
			}
			else if(year.equals("2014"))
			{
				return 3 % numReduceTasks;
			}
			else if(year.equals("2015"))
			{
				return 4 % numReduceTasks;
			}
			else
			{
				return 5 % numReduceTasks;
			}
		}
		
	}
	
	public static class Top5LocationsReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Integer, Text> top5LocationsInUs = new TreeMap<Integer,Text>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			int count = 0;
			String year = "";
			for(Text val : values)
			{
				year = val.toString();
				count++;
			}
			
			String location = key.toString();
			String total = String.format("%d", count);
			String myValue = year+"\t"+location+"\t"+total;
			
			top5LocationsInUs.put(new Integer(count), new Text(myValue));
			
			if(top5LocationsInUs.size() > 5)
			{
				top5LocationsInUs.remove(top5LocationsInUs.firstKey());
			}
		}
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException 
		{
			for(Text top5 : top5LocationsInUs.descendingMap().values())
			{
				context.write(NullWritable.get(), top5);
			}
		}
		
	}
	
	/*
	 * Driver class or Configuration code
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf  = new Configuration();
		
		if(args.length > 2)
		{
			conf.set("myText", args[2]);
		}
		else
		{
			System.out.println("Number of arguments should be 3");
			System.exit(0);
		}
		
		Job job = Job.getInstance(conf, "Top 5 locations in the US who have got certified visa for each year.");
		
		job.setJarByClass(Q2B_Top5LocationsInUSGotCertifiedVisaForEachYear.class);
		
		job.setMapperClass(Top5LocationsMapper.class);
		
		if(args[2].equals("ALL"))
		{
			job.setPartitionerClass(YearPartitioner.class);
			job.setNumReduceTasks(6);
		}
		
		job.setReducerClass(Top5LocationsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
