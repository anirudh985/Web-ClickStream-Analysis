package com.osu.ClickStreamAnalysis.MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class ClickStreamRecordReader extends RecordReader<UserTimestampWritable, LongWritable> {

	private LineReader in;
	private UserTimestampWritable key = new UserTimestampWritable();
	private LongWritable value = new LongWritable();
	private int maxLineLength;
	private long start;
    private long pos;
    private long end;
    
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		this.maxLineLength = conf.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
		
		start = split.getStart();
        end = start + split.getLength();
        
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);
        
        FSDataInputStream fileIn = fs.open(split.getPath());
		
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            --start;
            fileIn.seek(start);
        }
        
        in = new LineReader(fileIn, conf);
        

        if (skipFirstLine) {
            Text dummy = new Text();
			start += in.readLine(dummy, 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
			
        }
        this.pos = start;
			
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		int newSize = 0;
		 
        while (pos < end) {
        	
        		Text t = new Text();
			newSize = in.readLine(t, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
 
            if (newSize == 0) {
                break;
            }
 
            pos += newSize;
            
            String s = new String(t.getBytes());
            String[] keyValuePair = s.trim().split("\\s+");
            key.setUserId(new LongWritable(Long.parseLong(keyValuePair[0])));
            key.setTimestamp(new DoubleWritable(Double.parseDouble(keyValuePair[3])));
            value.set(Long.parseLong(keyValuePair[2]));
 
            if (newSize < maxLineLength) {
                break;
            }
 
        }
 
         
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
	}

	@Override
	public UserTimestampWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public LongWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
            in.close();
        }		
	}

}
