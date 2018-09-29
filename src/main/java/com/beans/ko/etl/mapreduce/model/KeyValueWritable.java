package com.beans.ko.etl.mapreduce.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class KeyValueWritable implements Writable{

	private String value;
	private String type;
	
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(value);
		out.writeUTF(type);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readUTF();
		type = in.readUTF();
	}
	
	
	
	
}
