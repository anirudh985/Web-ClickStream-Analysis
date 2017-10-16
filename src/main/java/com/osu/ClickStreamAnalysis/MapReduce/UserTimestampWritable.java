package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class UserTimestampWritable implements WritableComparable<UserTimestampWritable> {

	private LongWritable userId;
	private DoubleWritable timestamp;
	
	public UserTimestampWritable() {
		this.userId = new LongWritable(0);
		this.timestamp = new DoubleWritable(0);
	}
	
	public UserTimestampWritable(LongWritable userId, DoubleWritable timestamp) {
		this.userId = userId;
		this.timestamp = timestamp;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.userId.readFields(in);
		this.timestamp.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.userId.write(out);
		this.timestamp.write(out);
	}

	public int compareTo(UserTimestampWritable o) {
		int userCompare = this.userId.compareTo(o.userId);
		if(userCompare != 0) return userCompare;
		return this.timestamp.compareTo(o.timestamp);
	}

	public LongWritable getUserId() {
		return userId;
	}

	public DoubleWritable getTimestamp() {
		return timestamp;
	}

	public void setUserId(LongWritable userId) {
		this.userId = userId;
	}

	public void setTimestamp(DoubleWritable timestamp) {
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString() {
		return this.userId.toString() + Constants.SPACE + this.timestamp.toString();
	}

}
