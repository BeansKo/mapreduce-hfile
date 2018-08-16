package com.beans.ko.etl.mapreduce.utils;

import org.yaml.snakeyaml.Yaml;

import com.beans.ko.etl.mapreduce.model.FullExportConfig;
import com.beans.ko.etl.mapreduce.model.HiveConfig;
import com.beans.ko.etl.mapreduce.model.MapReduceConfig;

public class ConfigUtils {
	private static FullExportConfig CONFIG;
	private static MapReduceConfig MAP_REDUCE_CONFIG;
	private static HiveConfig HIVE_CONFIG;
	
	static{
		CONFIG = new Yaml().loadAs(ConfigUtils.class.getClassLoader().getResourceAsStream("application.yml"), FullExportConfig.class);
		MAP_REDUCE_CONFIG = CONFIG.getMapreduce();
		HIVE_CONFIG = CONFIG.getHive();
	}
	
	public static String getHadoopUserName(){
		return MAP_REDUCE_CONFIG.getHadoopUserName();
	}
	
	public static String getMapReduceJarPath(){
		return MAP_REDUCE_CONFIG.getJobJar();
	}
	
	public static String getHiveJdbcDriverClassName() {
		return HIVE_CONFIG.getJdbc().getDriverClassName();
	}
	
	public static String getHiveJdbcUrl() {
		return HIVE_CONFIG.getJdbc().getUrl();
	}
	
	public static String getHiveJdbcUsername() {
		return HIVE_CONFIG.getJdbc().getUsername();
	}
	
	public static String getHiveJdbcPassword() {
		return HIVE_CONFIG.getJdbc().getPassword();
	}
}
