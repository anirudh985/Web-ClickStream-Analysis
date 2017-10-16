package com.osu.ClickStreamAnalysis.MapReduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenerateClickStreamMapper extends Mapper<NullWritable, Text, NullWritable, Text> {

	@Override
	protected void map(NullWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		context.write(key, value);
	}

}
