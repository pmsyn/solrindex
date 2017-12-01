package com.db.connect.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author pms
 * @time 2017年11月1日上午11:38:03
*/
public class DatabaseConnectUtil {
	static Connection cn; 
	static Properties property = new Properties();
	
	public  Connection getLocalConnection () {
		if(cn == null) {
			try {
				property.load(this.getClass().getResourceAsStream("/db.properties"));
				String driver = property.getProperty("oracledriver");
				String url = property.getProperty("oracleurl");
				String username = property.getProperty("oracleuser");
				String password = property.getProperty("oraclepassword");
				Class.forName(driver);
				cn = DriverManager.getConnection(url, username, password);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return cn;
		
	}
	public  Connection getJshConnection () {
		if(cn == null) {
			try {
				property.load(this.getClass().getResourceAsStream("/db.properties"));
				String driver = property.getProperty("jshoracledriver");
				String url = property.getProperty("jshoracleurl");
				String username = property.getProperty("jshoracleuser");
				String password = property.getProperty("jshoraclepassword");
				Class.forName(driver);
				cn = DriverManager.getConnection(url, username, password);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return cn;
		
	}
	
	public static void closeConnection(Connection cn,PreparedStatement ps,ResultSet rs) {
		try {
			if(rs != null) {
				rs.close();
			}
			if(ps != null) {
				ps.close();
			}
			if(cn != null) {
				cn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}

