package com.beans.ko.etl.mapreduce.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Student implements Writable{
	private int id;
	private String studentName="";
	private String isStu="false";
	private String remark="";
	private int score;
	private String type="info";
	
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getStudentName() {
		return studentName;
	}
	public void setStudentName(String studentName) {
		this.studentName = studentName;
	}
	public String getIsStu() {
		return isStu;
	}
	public void setIsStu(String isStu) {
		this.isStu = isStu;
	}
	public String getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
	public int getScore() {
		return score;
	}
	public void setScore(int score) {
		this.score = score;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(studentName);
		out.writeUTF(isStu);
		out.writeUTF(remark);
		out.writeInt(score);
		out.writeUTF(type);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		studentName = in.readUTF();
		isStu = in.readUTF();
		remark = in.readUTF();
		score = in.readInt();
		type = in.readUTF();
	}
}
