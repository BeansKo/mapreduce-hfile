package com.beans.ko.etl.mapreduce.model;

public class MapReduceConfig {
	private String hadoopUserName;
    private String hbaseSnapshotPrefix;
    private String jobJar;
    private String libPathLocal;
    private String libPathRemote;
    
	public String getHadoopUserName() {
		return hadoopUserName;
	}
	public void setHadoopUserName(String hadoopUserName) {
		this.hadoopUserName = hadoopUserName;
	}
	public String getHbaseSnapshotPrefix() {
		return hbaseSnapshotPrefix;
	}
	public void setHbaseSnapshotPrefix(String hbaseSnapshotPrefix) {
		this.hbaseSnapshotPrefix = hbaseSnapshotPrefix;
	}
	public String getJobJar() {
		return jobJar;
	}
	public void setJobJar(String jobJar) {
		this.jobJar = jobJar;
	}
	public String getLibPathLocal() {
		return libPathLocal;
	}
	public void setLibPathLocal(String libPathLocal) {
		this.libPathLocal = libPathLocal;
	}
	public String getLibPathRemote() {
		return libPathRemote;
	}
	public void setLibPathRemote(String libPathRemote) {
		this.libPathRemote = libPathRemote;
	}
    
    
}
