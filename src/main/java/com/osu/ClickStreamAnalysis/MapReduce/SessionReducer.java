package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class SessionReducer extends Reducer<UserTimestampWritable, LongWritable, IntWritable, SessionCountsWritable> {
	
	private long startCategory;
	private long endCategory;
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startCategory = conf.getLong(Constants.START_CATEGORY, 0);
		endCategory = conf.getLong(Constants.END_CATEGORY, 0);
	}
	
	@Override
	protected void reduce(UserTimestampWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long noOfPagesInCurrentSession = 0;
		long noOfPagesVisited = 0;
		long noOfSessions = 0;
		boolean startCounting = false;
		Iterator<LongWritable> iterator = values.iterator();
		while(iterator.hasNext()) {
			long category = iterator.next().get();
			if(category == startCategory) {
				if(startCounting) {
					noOfPagesInCurrentSession = 0;
				}
				else {
					startCounting = true;
				}
			}
			else if(startCounting && category == endCategory) {
				noOfPagesVisited += noOfPagesInCurrentSession;
				noOfPagesInCurrentSession = 0;
				noOfSessions++;
				startCounting = false;
			}
			else if(startCounting) {
				noOfPagesInCurrentSession++;
			}
		}
		if(noOfSessions == 0) noOfPagesVisited = 0;
		context.write(one, new SessionCountsWritable(new LongWritable(noOfPagesVisited), new LongWritable(noOfSessions)));
	}
}
