package com.beans.ko.etl.mapreduce.utils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.orc.mapred.OrcTimestamp;
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
	
	public static String getHBaseSnapshotPrefix(){
		return MAP_REDUCE_CONFIG.getHbaseSnapshotPrefix();
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
	
	public static Set<String> getColumnFamilies(TableConfig table){
		return table.getColumns().stream().map(Column::getColumnFamily).collect(Collectors.toSet());
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
	
	public static String getOrcSchema(List<Column> fields) {
		StringBuffer schema = new StringBuffer();
		schema.append("struct<");
		Column field = fields.get(0);
		schema.append(field.getHiveColumn()).append(":").append(field.getType());
		for(int i=1; i<fields.size(); i++) {
			field = fields.get(i);
			schema.append(",").append(field.getHiveColumn()).append(":").append(field.getType());
		}
		schema.append(">");
		return schema.toString();
	}

	public static String getHBaseToHiveColumnMap(List<Column> fields) {
		StringBuffer columnMap = new StringBuffer();
		for(Column c : fields) {
			columnMap.append(",")
				.append(c.getColumnFamily()).append(":").append(c.getQualifier())
				.append("|")
				.append(c.getHiveColumn());
		}
		return columnMap.substring(1);
	}
	
	public static WritableComparable<?> convertValue(TypeDescription type, byte[] value) {
		Category c = type.getCategory();
		switch(c) {
			case BOOLEAN:
				if(value.length == 1) {
					return new BooleanWritable(Bytes.toBoolean(value));
				} else {
					return new BooleanWritable(Boolean.parseBoolean(Bytes.toString(value)));
				}
			case BYTE:
				if(value.length == 1) {
					return new ByteWritable(value[0]);
				} else {
					return new ByteWritable(Byte.valueOf(Bytes.toString(value)));
				}
			case SHORT:
				try {
					return new ShortWritable(Short.valueOf(Bytes.toString(value)));
				} catch (NumberFormatException e) {
					return new ShortWritable(Bytes.toShort(value));
				}
			case INT:
				try {
					return new IntWritable(Integer.valueOf(Bytes.toString(value)));
				} catch (NumberFormatException e) {
					return new IntWritable(Bytes.toInt(value));
				}
			case LONG:
				try {
					return new LongWritable(Long.parseLong(Bytes.toString(value)));
				} catch (NumberFormatException e) {
					return new LongWritable(Bytes.toLong(value));
				}
			case FLOAT:
				try {
					return new FloatWritable(Float.parseFloat(Bytes.toString(value)));
				} catch (NumberFormatException e) {
					return new FloatWritable(Bytes.toFloat(value));
				}
			case DOUBLE:
				try {
					return new DoubleWritable(Double.parseDouble(Bytes.toString(value)));
				} catch (NumberFormatException e) {
					return new DoubleWritable(Bytes.toDouble(value));
				}
			case STRING:
			case VARCHAR:
			case CHAR:
				return new Text(Bytes.toString(value));
			case DATE:
				try {
					return new DateWritable(new java.sql.Date(Long.parseLong(Bytes.toString(value))));
				} catch (NumberFormatException e) {
					return new DateWritable(new java.sql.Date(Bytes.toLong(value)));
				}
			case TIMESTAMP:
				try {
					return new OrcTimestamp(Long.parseLong(Bytes.toString(value)));
				} catch (NumberFormatException e) {
					return new OrcTimestamp(Bytes.toLong(value));
				}
			case BINARY:
				return new BytesWritable(value);
			case DECIMAL:
				try {
					return new HiveDecimalWritable(HiveDecimal.create(new BigDecimal(Bytes.toString(value))));
				} catch (NumberFormatException e) {
					return new HiveDecimalWritable(HiveDecimal.create(Bytes.toBigDecimal(value)));
				}
			case LIST:
			case MAP:
			case STRUCT:
			case UNION:
				String errorMsg = String.format("Can't support %s, %s, %s, %s type convertion",
						Category.LIST.getName(),
						Category.MAP.getName(),
						Category.STRUCT.getName(),
						Category.UNION.getName()
					);
				throw new IllegalArgumentException(errorMsg);
			default:
				throw new IllegalArgumentException("Unknown type " + c.getName());
		}
	}
}
