package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AveragePagesReducer extends Reducer<IntWritable, SessionCountsWritable, NullWritable, DoubleWritable> {
	
	@Override
	protected void reduce(IntWritable key, Iterable<SessionCountsWritable> values, Context context)
			throws IOException, InterruptedException {
		long totalNoOfPagesVisited = 0;
		long totalNoOfSessions = 0;
		
		SessionCountsWritable currentCounts;
		Iterator<SessionCountsWritable> iterator = values.iterator();
		while(iterator.hasNext()) {
			currentCounts = iterator.next();
			totalNoOfPagesVisited += currentCounts.getNoOfPagesVisited().get();
			totalNoOfSessions += currentCounts.getNoOfSessions().get();
		}
		context.write(NullWritable.get(), new DoubleWritable(totalNoOfPagesVisited / (double)totalNoOfSessions));
	}
}
