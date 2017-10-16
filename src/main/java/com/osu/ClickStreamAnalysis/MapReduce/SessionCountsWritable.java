package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class SessionCountsWritable implements Writable {

	private LongWritable noOfPagesVisited;
	private LongWritable noOfSessions;
	
	public SessionCountsWritable() {
		this.noOfPagesVisited = new LongWritable(0);
		this.noOfSessions = new LongWritable(0);
	}
	
	public SessionCountsWritable(LongWritable noOfPagesVisited, LongWritable noOfSessions) {
		this.noOfPagesVisited = noOfPagesVisited;
		this.noOfSessions = noOfSessions;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.noOfPagesVisited.readFields(in);
		this.noOfSessions.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.noOfPagesVisited.write(out);
		this.noOfSessions.write(out);
	}

	public LongWritable getNoOfPagesVisited() {
		return noOfPagesVisited;
	}

	public LongWritable getNoOfSessions() {
		return noOfSessions;
	}

	public void setNoOfPagesVisited(LongWritable noOfPagesVisited) {
		this.noOfPagesVisited = noOfPagesVisited;
	}

	public void setNoOfSessions(LongWritable noOfSessions) {
		this.noOfSessions = noOfSessions;
	}
	
	@Override
	public String toString() {
		return this.noOfPagesVisited.toString() + Constants.SPACE + this.noOfSessions.toString();
	}

}
