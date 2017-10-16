package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SessionCountsInputFormat extends FileInputFormat<IntWritable, SessionCountsWritable> {

	@Override
	public RecordReader<IntWritable, SessionCountsWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new SessionCountsRecordReader();
	}

}
