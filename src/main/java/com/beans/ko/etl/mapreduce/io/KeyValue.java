package com.beans.ko.etl.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyValue implements Serializable, Cloneable, WritableComparable<KeyValue> {
	private static final long serialVersionUID = 6816606933757056160L;
	private Text key;
	private BytesWritable value;

	public KeyValue() {
		this.put(new Text(), new BytesWritable());
	}

	public void put(Text key, BytesWritable value) {
		this.key = key;
		this.value = value;
	}

	public Text getKey() {
		return key;
	}
	public void setKey(Text key) {
		this.key = key;
	}
	public BytesWritable getValue() {
		return value;
	}
	public void setValue(BytesWritable value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		this.value.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in);
		this.value.readFields(in);
	}
	@Override
	public int compareTo(KeyValue o) {
		return this.key.compareTo(o.getKey());
	}
}
