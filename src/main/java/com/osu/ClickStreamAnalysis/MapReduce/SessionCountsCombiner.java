package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SessionCountsCombiner extends Reducer<IntWritable, SessionCountsWritable, IntWritable, SessionCountsWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<SessionCountsWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long sumOfPages = 0;
		long sumOfSessions = 0;
		
		for(SessionCountsWritable value : values) {
			sumOfPages += value.getNoOfPagesVisited().get();
			sumOfSessions += value.getNoOfSessions().get();
		}

		context.write(key, new SessionCountsWritable(new LongWritable(sumOfPages), new LongWritable(sumOfSessions)));
	}

}
