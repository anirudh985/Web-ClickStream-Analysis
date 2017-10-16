package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable{
	
	
	public void readFields(DataInput arg0) throws IOException {
	}

	public void write(DataOutput arg0) throws IOException {
	}

	public long getLength() throws IOException {
		return 0;
	}

	public String[] getLocations() throws IOException {
		return new String[0];
	}

}
