package com.beans.ko.etl.mapreduce.model;

public class FullExportConfig {
	private MapReduceConfig mapreduce;
	private HiveConfig hive;

	public HiveConfig getHive() {
		return hive;
	}

	public void setHive(HiveConfig hive) {
		this.hive = hive;
	}

	public MapReduceConfig getMapreduce() {
		return mapreduce;
	}

	public void setMapreduce(MapReduceConfig mapreduce) {
		this.mapreduce = mapreduce;
	}
	
}
