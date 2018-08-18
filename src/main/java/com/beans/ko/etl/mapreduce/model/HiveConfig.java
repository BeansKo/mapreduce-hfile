package com.beans.ko.etl.mapreduce.model;

public class HiveConfig {
	
	private HiveJdbcConfig jdbc;
	private HiveTableConfig table;
	
	public HiveJdbcConfig getJdbc() {
		return jdbc;
	}


	public void setJdbc(HiveJdbcConfig jdbc) {
		this.jdbc = jdbc;
	}

	public HiveTableConfig getTable() {
		return table;
	}


	public void setTable(HiveTableConfig table) {
		this.table = table;
	}

	public static class HiveJdbcConfig{
		private String driverClassName;
		private String url;
		private String username;
		private String password;
		
		
		public String getDriverClassName() {
			return driverClassName;
		}
		public void setDriverClassName(String driverClassName) {
			this.driverClassName = driverClassName;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public String getPassword() {
			return password;
		}
		public void setPassword(String password) {
			this.password = password;
		}
	}

	public static class HiveTableConfig{
		private String outputPathRemote;
		private String partitionColumn;
		
		public String getOutputPathRemote() {
			return outputPathRemote;
		}
		public void setOutputPathRemote(String outputPathRemote) {
			this.outputPathRemote = outputPathRemote;
		}
		public String getPartitionColumn() {
			return partitionColumn;
		}
		public void setPartitionColumn(String partitionColumn) {
			this.partitionColumn = partitionColumn;
		}
		
	}
}
