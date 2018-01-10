package com.niit.h1bvisa;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q6_CaseStatusPercentOnTotalApplicationYearWise {

	/*
	 * Mapper code
	 */
	public static class PercentMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String myTextSearch = context.getConfiguration().get("myText");
			
			String[] record = value.toString().split("\t");
			String case_status = record[1];
			String year = record[7];
			
			if(myTextSearch.equals("ALL"))
			{
				context.write(new Text(year), new Text(case_status));
			}
			else
			{
				if(year.equals(myTextSearch))
				{
					context.write(new Text(year), new Text(case_status));
				}
			}
		}	
	}
	
	/*
	 * Reducer Code
	 */
	public static class PercentReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			double allcount = 0;
			double certifiedcount = 0;
			double deniedcount = 0;
			double withdrawncount = 0;
			double certified_withdrawncount = 0;
			
			double cert_percent = 0.0;
			double certwith__percent =0.0;
			double with_percent = 0.0;
			double deni_percent = 0.0;
			
			String case_status = "";
			
			for(Text val : values)
			{
				case_status = val.toString();
				
				allcount++;
				
				if(case_status.equals("CERTIFIED-WITHDRAWN"))
				{
					certified_withdrawncount++;
				}
				if(case_status.equals("CERTIFIED"))
				{
					certifiedcount++;
				}
			    if(case_status.equals("WITHDRAWN"))
				{
					withdrawncount++;
				}
				if(case_status.equals(("DENIED")))
				{
					deniedcount++;
				}	
			}
			
			 cert_percent = (certifiedcount * 100)/allcount;
			 
			 certwith__percent = (certified_withdrawncount*100)/allcount;
			 
			 with_percent = (withdrawncount*100)/allcount;
			 
			 deni_percent = (deniedcount*100)/allcount;
			
			 String certTotal = String.format("%.2f", certifiedcount);
			 String certwithTotal = String.format("%.2f", certified_withdrawncount);
			 String withTotal = String.format("%.2f", withdrawncount);
			 String deniedTotal = String.format("%.2f", deniedcount);
			 
			 String certPercent = String.format("%.2f", cert_percent);
			 String certwithPercent = String.format("%.2f", certwith__percent);
			 String withPercent = String.format("%.2f", with_percent);
			 String deninedPercent = String.format("%.2f", deni_percent);
			 
			String myValue =  certTotal+","+certPercent+","+ certwithTotal+","+certwithPercent+","+withTotal+","+withPercent+","+deniedTotal+","+deninedPercent;
			
			context.write(key, new Text(myValue));
		}
	}
	
		/*
		 * Driver class or Configuration code
		 */
		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			
			conf.set("mapreduce.output.textoutputformat.separator",",");
			
			if(args.length > 2)
			{
				conf.set("myText", args[2]);
			}
			else
			{
				System.out.println("Number of arguments should be 3");
				System.exit(0);
			}
			
			Job job = Job.getInstance(conf,"Percentage and Count of each case status on total Applications for each year");
			
			job.setJarByClass(Q6_CaseStatusPercentOnTotalApplicationYearWise.class);
			
			job.setMapperClass(PercentMapper.class);
			job.setReducerClass(PercentReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
