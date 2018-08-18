package com.beans.ko.etl.mapreduce.model;

import java.util.List;

public class FullExportConfig {
	private MapReduceConfig mapreduce;
	private HiveConfig hive;
	private List<TableConfig> tables;

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

	public List<TableConfig> getTables() {
		return tables;
	}

	public void setTables(List<TableConfig> tables) {
		this.tables = tables;
	}
}
