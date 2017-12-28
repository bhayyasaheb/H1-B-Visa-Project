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

public class Q3_UsingTreeMap_DataScientist {

	/*
	 * Mapper code
	 */
	public static class DataScientistMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		LongWritable one = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			String job_title = record[4];
			String case_status = record[1];
			String soc_name = record[3];
			
			if(job_title.contains("DATA SCIENTIST") && case_status.equals("CERTIFIED"))
			{
				context.write(new Text(soc_name), one);
			}
			
		}
		
	}
	
	/*
	 * Reducer code
	 */
	public static class DataScientistReducer extends Reducer<Text, LongWritable, NullWritable, Text>
	{
		TreeMap<Long, Text> dataScientistJobs  = new TreeMap<Long,Text>();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException, InterruptedException 
		{
			long sum =0;
			String myTotal = "";
			String myValue = "";
			
			for(LongWritable val : values)
			{
				sum += val.get();
			}
			
			long totalcount = sum;
			myValue = key.toString();
		    myTotal = String.format("%d", sum);
		   
		    myValue = myValue +","+myTotal;
			
			dataScientistJobs.put(new Long(totalcount), new Text(myValue));
			
			if(dataScientistJobs.size() > 5)
			{
				dataScientistJobs.remove(dataScientistJobs.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			for(Text t : dataScientistJobs.descendingMap().values())
			{
				context.write(NullWritable.get(), t);
			}
		}
		
	}
	
	/*
	 * Driver class or Configuration code
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Most Number of Data Scientist Position Using Tree Map");
		
		job.setJarByClass(Q3_UsingTreeMap_DataScientist.class);
		
		job.setMapperClass(DataScientistMapper.class);
		job.setReducerClass(DataScientistReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
