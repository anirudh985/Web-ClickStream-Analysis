package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.osu.ClickStreamAnalysis.Common.Constants;

public class GenerateClickStreamRecordReader extends RecordReader<NullWritable, Text>{

	
	private int createdRecords = 0;
	private NullWritable key = NullWritable.get();
	private Text value = new Text();
	private Random random = new Random();
	private long noOfClickStreamsToGenerate;
	private int numSplits;
	private long noOfClickStreamsToGenerateBySplit;
	private long rangeOfNumbersToGenerate;
	
	private long numberOfUsers;
	private long numberOfPages;
	private long numberOfCategories;
	private long timeRange;

	private Random randomUserGenerator = new Random(1234567);
	private Random randomPageGenerator = new Random(3456712);
	private Random randomCategoryGenerator = new Random(5671234);
	private Random randomTimestampGenerator = new Random(9871234);
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		this.numberOfUsers = conf.getLong(Constants.NO_OF_USERS, 0);
		this.numberOfPages = conf.getLong(Constants.NO_OF_PAGES, 0);
		this.numberOfCategories = conf.getLong(Constants.NO_OF_CATEGORIES, 0);
		this.timeRange = conf.getLong(Constants.TIME_RANGE, 0);
		
		this.noOfClickStreamsToGenerate = this.numberOfUsers * 1000;
		this.numSplits = conf.getInt(Constants.NO_OF_MAPPERS_TO_GENERATE_DATA, 0);
		this.noOfClickStreamsToGenerateBySplit = this.noOfClickStreamsToGenerate / this.numSplits;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(createdRecords < noOfClickStreamsToGenerateBySplit) {
			long randomUser = Math.abs(randomUserGenerator.nextLong()) % this.numberOfUsers + 1;
			long randomPage = Math.abs(randomPageGenerator.nextLong()) % this.numberOfPages + 1;
			long randomCategory = Math.abs(randomCategoryGenerator.nextLong()) % this.numberOfCategories + 1;
			float randomTimestamp = 1 + Math.abs(randomTimestampGenerator.nextFloat()) * (this.timeRange - 1);
			String value = String.valueOf(randomUser) + Constants.SPACE + String.valueOf(randomPage) + Constants.SPACE
					+ String.valueOf(randomCategory) + Constants.SPACE + String.valueOf(randomTimestamp);
			this.value.set(value);
			this.createdRecords++;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) createdRecords / (float) noOfClickStreamsToGenerateBySplit;
	}

	@Override
	public void close() throws IOException {
		
	}
	

}
