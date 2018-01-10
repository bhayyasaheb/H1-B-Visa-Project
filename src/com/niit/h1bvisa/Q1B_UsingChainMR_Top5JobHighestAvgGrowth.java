package com.niit.h1bvisa;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1B_UsingChainMR_Top5JobHighestAvgGrowth {

	public static class Top5JobMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");

			String job_title = record[4];
			String year = record[7];

			context.write(new Text(job_title), new Text(year));
		}
	}

	public static class Top5JobReducer extends Reducer<Text, Text, DoubleWritable, Text> 
	{
		TreeMap<Double, Text> top5Job = new TreeMap<Double, Text>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int count2011 = 0;
			int count2012 = 0;
			int count2013 = 0;
			int count2014 = 0;
			int count2015 = 0;
			int count2016 = 0;

			double growth2012 = 0.0;
			double growth2013 = 0.0;
			double growth2014 = 0.0;
			double growth2015 = 0.0;
			double growth2016 = 0.0;

			double averageGrowth = 0.0;

			for (Text val : values) {
				
				String year = val.toString();

				if (year.equals("2011")) {
					count2011++;
				} else if (year.equals("2012")) {
					count2012++;
				} else if (year.equals("2013")) {
					count2013++;
				} else if (year.equals("2014")) {
					count2014++;
				} else if (year.equals("2015")) {
					count2015++;
				} else if (year.equals("2016")){
					count2016++;
				}
			}

			if (count2011 != 0) {
				growth2012 = (double)(count2012 - count2011) * 100 / (double)count2011;
			} else {
				growth2012 = 0;
			}

			if (count2012 != 0) {
				growth2013 = (double)(count2013 - count2012) * 100 / (double)count2012;
			} else {
				growth2013 = 0;
			}

			if (count2013 != 0) {
				growth2014 = (double)(count2014 - count2013) * 100 / (double)count2013;
			} else {
				growth2014 = 0;
			}

			if (count2014 != 0) {
				growth2015 = (double)(count2015 - count2014) * 100 / (double)count2014;
			} else {
				growth2015 = 0;
			}

			if (count2015 != 0) {
				growth2016 = (double)(count2016 - count2015) * 100 / (double)count2015;
			} else {
				growth2016 = 0;
			}

			averageGrowth = (growth2012 + growth2013 + growth2014 + growth2015 + growth2016) / 5;
			
			//String highestAvgGrowth = String.format("%f", averageGrowth);
			/*String total2011 = String.format("%d", count2011);
			String total2012 = String.format("%d", count2012);
			String total2013 = String.format("%d", count2013);
			String total2014 = String.format("%d", count2014);
			String total2015 = String.format("%d", count2015);
			String total2016 = String.format("%d", count2016);*/
			
			
			String job_title = key.toString();
			
			//String myValue = job_title+"\t"+total2011+"\t"+total2012+"\t"+total2013+"\t"+total2014+"\t"+total2015+"\t"+total2016;
			
			//context.write(new DoubleWritable(averageGrowth), new Text(myValue));
			context.write(new DoubleWritable(averageGrowth), new Text(job_title));
			
		}
	}

	public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			
			double  averageGrowth = Double.parseDouble(record[0]);
			String job_title = record[1];
			/*String total2011 = record[2];
			String total2012 = record[3];
			String total2013 = record[4];
			String total2014 = record[5];
			String total2015 = record[6];
			String total2016 = record[7];*/
			
			//String myValue = job_title+"\t"+total2011+"\t"+total2012+"\t"+total2013+"\t"+total2014+"\t"+total2015+"\t"+total2016;
			
			//context.write(new DoubleWritable(averageGrowth), new Text(myValue));
			context.write(new DoubleWritable(averageGrowth), new Text(job_title));
		}	
	}
	
	public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{
		int count = 0;
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			if(count < 5)
			{
				for(Text val :values)
				{
					context.write(new Text(val), key);
					count++;
				}
			}
			
		}	
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf,"Top 5 job titles who are having highest avg growth in applications");

		job.setJarByClass(Q1B_UsingChainMR_Top5JobHighestAvgGrowth.class);

		job.setMapperClass(Top5JobMapper.class);
		job.setReducerClass(Top5JobReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		Path outputpath1 = new Path("FirstMapper2");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputpath1);
		
		FileSystem.get(conf).delete(outputpath1, true);
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf,"Top 5 job titles who are having highest avg growth in applications");

		job2.setJarByClass(Q1B_UsingChainMR_Top5JobHighestAvgGrowth.class);
		
		job2.setMapperClass(SortMapper.class);
		
		job2.setSortComparatorClass(DecreasingComparator.class);
		
		job2.setReducerClass(SortReducer.class);
		
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
