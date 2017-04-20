package com.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import com.project.PreProcess.PreProcessMapper;


public class Driver {
	
	public static void main(String args[]) throws Exception {
		BasicConfigurator.configure();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: driver <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "preprocessing");
		job.setJarByClass(PreProcess.class);
		job.setMapperClass(PreProcessMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		// Intermediate Output directory
		FileOutputFormat.setOutputPath(job, new Path("CleanData"));
		job.waitForCompletion(true);

	}
}