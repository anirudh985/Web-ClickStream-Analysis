package com.osu.ClickStreamAnalysis.MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class UserPartitioner extends HashPartitioner<UserTimestampWritable, LongWritable> {

	@Override
	public int getPartition(UserTimestampWritable key, LongWritable value, int numPartitions) {
		return (key.getUserId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
