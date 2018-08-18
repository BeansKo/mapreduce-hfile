package com.beans.ko.etl.mapreduce.utils;

import static com.beans.ko.etl.mapreduce.utils.ConfigUtils.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveUtils {
	static {
		try {
			//注册驱动
			Class.forName(getHiveJdbcDriverClassName());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static Connection getConnection(String url,String username,String password) throws SQLException{
		return  DriverManager.getConnection(url,username,password);
	}
	
	public static void executeQuery(String hql){
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = getConnection(getHiveJdbcUrl(),getHiveJdbcUsername(),getHiveJdbcPassword());
			stmt = conn.createStatement();
			stmt.execute(hql);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null) { stmt.close(); }
				if (conn != null) { conn.close(); }
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
