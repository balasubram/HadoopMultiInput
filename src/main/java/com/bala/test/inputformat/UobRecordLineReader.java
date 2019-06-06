package com.bala.test.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class UobRecordLineReader extends RecordReader<LongWritable, Text> {

	private boolean stillInChunk = true;

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	private DataOutputBuffer dataReadBuffer = new DataOutputBuffer();
	private byte[] beginRecordString = "D\\u0007".getBytes();
	private FSDataInputStream fsDataInputStream;

	private long splitstart = 0;
	private long splitend = 0;
	private long bytesread = 0;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {

		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = taskAttemptContext.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		fsDataInputStream = fs.open(path);
		fsDataInputStream.seek(split.getStart());
		this.splitstart = split.getStart();
		this.splitend = this.splitstart + split.getLength();

		if (splitstart != 0) {
			skipAndReadRecord(beginRecordString, false);
		}

	}

	@Override
	public boolean nextKeyValue() throws IOException {
		if (!stillInChunk)
			return false;

		boolean status = skipAndReadRecord(beginRecordString, true);
		value = new Text();
		value.set(dataReadBuffer.getData(), 0, dataReadBuffer.getLength());
		key = new LongWritable(fsDataInputStream.getPos());
		dataReadBuffer.reset();
		if (!status) {
			stillInChunk = false;
		}
		return true;
	}

	@Override
	public void close() throws IOException {
		fsDataInputStream.close();
	}

	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException {
		if (splitstart == splitend) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (splitstart + bytesread - splitstart) / (float) (splitend - splitstart));
		}
	}

	private boolean skipAndReadRecord(byte[] nextRecord, boolean withinBlock) throws IOException {
		int i = 0;
		while (true) {
			int b = fsDataInputStream.read();
			if (b == -1)
				return false;
			if (b == nextRecord[i]) {
				i++;
				if (i >= nextRecord.length) {
					return fsDataInputStream.getPos() < splitend;
				}
			} else {
				if (withinBlock) {
					dataReadBuffer.write(b);
				}
			}
		}
	}

}