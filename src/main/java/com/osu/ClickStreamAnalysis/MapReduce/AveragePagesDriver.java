package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class AveragePagesDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		/************************ MR0 ******************************/
		
		long numberOfUsers = Long.parseLong(args[0]);
		long numberOfPages = Long.parseLong(args[1]);
		long numberOfCategories = Long.parseLong(args[2]);
		long timeRange = Long.parseLong(args[3]);
		int numberOfMappersToGenerateData = Integer.parseInt(args[4]);
		
		Configuration conf0 = new Configuration();
		conf0.setLong(Constants.NO_OF_USERS, numberOfUsers);
		conf0.setLong(Constants.NO_OF_PAGES, numberOfPages);
		conf0.setLong(Constants.NO_OF_CATEGORIES, numberOfCategories);
		conf0.setLong(Constants.TIME_RANGE, timeRange);
		conf0.setInt(Constants.NO_OF_MAPPERS_TO_GENERATE_DATA, numberOfMappersToGenerateData);
		
		Job job0 = Job.getInstance(conf0, "Generate ClickSteam Data");
		job0.setJarByClass(AveragePagesDriver.class);
		
		Path outputPath0 = new Path(args[5]);
		
		job0.setInputFormatClass(GenerateClickStreamInputFormat.class);
		job0.setMapOutputKeyClass(NullWritable.class);
		job0.setMapOutputValueClass(Text.class);
		
		job0.setMapperClass(GenerateClickStreamMapper.class);
		FileOutputFormat.setOutputPath(job0, outputPath0);
		job0.waitForCompletion(true);

		/************************ MR0 ******************************/
		
		/************************ MR1 ******************************/
		
		long startCategory = Long.parseLong(args[7]);
		long endCategory = Long.parseLong(args[8]);
		int numberOfReducers = Integer.parseInt(args[9]);
		
		Configuration conf1 = new Configuration();
		
		conf1.setLong(Constants.START_CATEGORY, startCategory);
		conf1.setLong(Constants.END_CATEGORY, endCategory);
		Job job1 = Job.getInstance(conf1, "User Session Values");
		job1.setJarByClass(AveragePagesDriver.class);
		
		Path inputPath1 = outputPath0;
		Path outputPath1 = new Path(Constants.MR1_OUTPUT_PATH);
		
		job1.setNumReduceTasks(numberOfReducers);
		FileInputFormat.setInputPaths(job1, inputPath1);
		
		
		job1.setInputFormatClass(ClickStreamInputFormat.class);
		job1.setMapOutputKeyClass(UserTimestampWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);
		
		job1.setPartitionerClass(UserPartitioner.class);
		job1.setGroupingComparatorClass(UserTimestampGroupingComparator.class);
		job1.setReducerClass(SessionReducer.class);
		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(SessionCountsWritable.class);
		FileOutputFormat.setOutputPath(job1, outputPath1);
		job1.waitForCompletion(true);
		
		/************************ MR1 ******************************/
		
		/************************ MR2 ******************************/
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Average Pages");
		job2.setJarByClass(AveragePagesDriver.class);
		
		Path inputPath2 = outputPath1;
		Path outputPath2 = new Path(args[6]);
		
		job2.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job2, inputPath2);
		
		job2.setInputFormatClass(SessionCountsInputFormat.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(SessionCountsWritable.class);
		
		job2.setCombinerClass(SessionCountsCombiner.class);
		job2.setReducerClass(AveragePagesReducer.class);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
		/************************ MR2 ******************************/
		
	}

}
