package com.beans.ko.etl.mapreduce.model;

import java.util.List;

public class TableConfig {
	/** HBase Table Name */
	private String hbaseTableName;
	/** Hive Table Name */
	private String hiveTableName;
	/** MapReduce maxSplitSize Config */
	private long maxSplitSize;
	/** MapReduce reduceNumber */
	private long reduceNumber;
	/** HBase Columns */
	private List<Column> columns;
	
	public String getHbaseTableName() {
		return hbaseTableName;
	}

	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}

	public String getHiveTableName() {
		return hiveTableName;
	}

	public void setHiveTableName(String hiveTableName) {
		this.hiveTableName = hiveTableName;
	}

	public long getMaxSplitSize() {
		return maxSplitSize;
	}

	public void setMaxSplitSize(long maxSplitSize) {
		this.maxSplitSize = maxSplitSize;
	}

	public long getReduceNumber() {
		return reduceNumber;
	}

	public void setReduceNumber(long reduceNumber) {
		this.reduceNumber = reduceNumber;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}


	public static class Column{
		/** HBase Column Family */
		private String columnFamily;
		/** HBase Qualifier */
		private String qualifier;
		/**
		 * Date Type Support:
		 * boolean
		 * tinyint
		 * smallint
		 * int
		 * bigint
		 * float
		 * double
		 * string
		 * varchar
		 * char
		 * date
		 * timestamp
		 * binary
		 * decimal
		 */
		private String type;
		/** Hive ColumnName */
		private String hiveColumn;
		
		public String getColumnFamily() {
			return columnFamily;
		}
		public void setColumnFamily(String columnFamily) {
			this.columnFamily = columnFamily;
		}
		public String getQualifier() {
			return qualifier;
		}
		public void setQualifier(String qualifier) {
			this.qualifier = qualifier;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public String getHiveColumn() {
			return hiveColumn;
		}
		public void setHiveColumn(String hiveColumn) {
			this.hiveColumn = hiveColumn;
		}
	}
}
