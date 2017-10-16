package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ClickStreamInputFormat extends FileInputFormat<UserTimestampWritable, LongWritable> {

	@Override
	public RecordReader<UserTimestampWritable, LongWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new ClickStreamRecordReader();
	}

}
