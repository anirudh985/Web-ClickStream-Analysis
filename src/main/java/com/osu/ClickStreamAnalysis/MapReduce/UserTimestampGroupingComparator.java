package com.osu.ClickStreamAnalysis.MapReduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class UserTimestampGroupingComparator extends WritableComparator {
	
	protected UserTimestampGroupingComparator() {
		super(UserTimestampWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserTimestampWritable key1 = (UserTimestampWritable) w1;
		UserTimestampWritable key2 = (UserTimestampWritable) w2;
		return key1.getUserId().compareTo(key2.getUserId());
	}
	
	

}
