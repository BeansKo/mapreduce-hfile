package com.beans.ko.etl.mapreduce.hfile;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * 实现RecordReader
 * @author fl76
 *
 */
public class HFileRecordReader extends RecordReader<ImmutableBytesWritable, KeyValue> {

	private HFile.Reader reader;
	private HFileScanner scanner;
	private FileSystem fs;
	private Path path;
	private int keyNumber = 0;
	private boolean isLastOne;
	public HFileRecordReader(CombineFileSplit split,TaskAttemptContext context,Integer index) throws IOException{
		fs = FileSystem.get(context.getConfiguration());
		path = split.getPath(index);
		boolean exist = fs.exists(path);
		if(exist && reader == null){
			reader = HFile.createReader(fs, path, 
					new CacheConfig(context.getConfiguration()), 
					context.getConfiguration());
			reader.loadFileInfo();
			scanner = reader.getScanner(false, false);
			scanner.seekTo();
					
		}
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {		
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		keyNumber +=1;
		return !isLastOne;
	}
	@Override
	public ImmutableBytesWritable getCurrentKey() throws IOException,
			InterruptedException {
		CellUtil.cloneRow(scanner.getKeyValue());
//		return new ImmutableBytesWritable(scanner.getKeyValue().getRow());
		return new ImmutableBytesWritable(CellUtil.cloneRow(scanner.getKeyValue()));
	}
	@Override
	public KeyValue getCurrentValue() throws IOException, InterruptedException {
		Cell cell = scanner.getKeyValue();
		KeyValue kv = new KeyValue(cell);
		if(!scanner.next()){
			isLastOne = true;
		}
		return kv;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return keyNumber/reader.getEntries();
	}
	@Override
	public void close() throws IOException {
		if(reader != null){
			reader.close();
		}
		
	}
}
