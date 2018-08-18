package com.beans.ko.etl.mapreduce.utils;

import java.util.List;

import org.yaml.snakeyaml.Yaml;

import com.beans.ko.etl.mapreduce.model.FullExportConfig;
import com.beans.ko.etl.mapreduce.model.HiveConfig;
import com.beans.ko.etl.mapreduce.model.MapReduceConfig;
import com.beans.ko.etl.mapreduce.model.TableConfig;
import com.beans.ko.etl.mapreduce.model.TableConfig.Column;

public class ConfigUtils {
	private static FullExportConfig CONFIG;
	private static MapReduceConfig MAP_REDUCE_CONFIG;
	private static HiveConfig HIVE_CONFIG;
	private static List<TableConfig> TABLES;

	static {
		CONFIG = new Yaml()
				.loadAs(ConfigUtils.class.getClassLoader().getResourceAsStream(
						"application.yml"), FullExportConfig.class);
		MAP_REDUCE_CONFIG = CONFIG.getMapreduce();
		HIVE_CONFIG = CONFIG.getHive();
		TABLES = CONFIG.getTables();
	}

	public static String getHadoopUserName() {
		return MAP_REDUCE_CONFIG.getHadoopUserName();
	}

	public static String getMapReduceJarPath() {
		return MAP_REDUCE_CONFIG.getJobJar();
	}
	
	public static String getMapReduceLibPathLocal(){
		return MAP_REDUCE_CONFIG.getLibPathLocal();
	}
	
	public static String getMapReduceLibPathRemote(){
		return MAP_REDUCE_CONFIG.getLibPathRemote();
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

	public static String getHiveTableOutputPathRemote() {
		String output = HIVE_CONFIG.getTable().getOutputPathRemote();
		return output.endsWith("/") ? output.substring(0, output.length() - 1)
				: output;
	}

	public static String getHiveTablePartitionColumn() {
		return HIVE_CONFIG.getTable().getPartitionColumn();
	}

	public static List<TableConfig> getTables() {
		return TABLES;
	}

	public static String getHiveCreateTalSql(TableConfig table,
			String tablePath, String partitionColumn) {
		List<Column> fields = table.getColumns();
		StringBuffer createSql = new StringBuffer();
		createSql.append("create external table if not exists ")
				.append("fl76_"+table.getHiveTableName()).append(" (\n");
		Column field = fields.get(0);
		createSql.append("`").append(field.getHiveColumn()).append("` ")
				.append(field.getType().toLowerCase()).append("\n");
		for (int i = 1; i < fields.size(); i++) {
			field = fields.get(i);
			createSql.append(",`").append(field.getHiveColumn()).append("` ")
					.append(field.getType().toLowerCase()).append("\n");
		}
		createSql.append(")\n")
				.append("partitioned by (" + partitionColumn + " string)\n")
				.append("stored as orc\n").append("location '")
				.append(tablePath).append("'");
		return createSql.toString();
	}
}
