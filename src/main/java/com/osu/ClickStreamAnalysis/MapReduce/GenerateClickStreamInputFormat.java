package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class GenerateClickStreamInputFormat extends InputFormat<NullWritable, Text>{

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		int numSplits = conf.getInt(Constants.NO_OF_MAPPERS_TO_GENERATE_DATA, 5);
		
		List<InputSplit> splits = new ArrayList<>();
		
		for(int split = 0; split < numSplits; split++) {
			splits.add(new FakeInputSplit());
		}
		
		return splits;
	}

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		GenerateClickStreamRecordReader recordReader = new GenerateClickStreamRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}
	
	

}
