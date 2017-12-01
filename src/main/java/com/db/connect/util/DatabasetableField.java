package com.db.connect.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author pms
 * @time 2017��11��28������5:16:57
*/
public class DatabasetableField {
	
	/**
	 * ��ȡ�ֶ�ֵ
	 * @param rs
	 * @param rsmd
	 * @param cols
	 * @return
	 * @throws SQLException
	 */
	public static Object getResultFieldValue(ResultSet rs, ResultSetMetaData rsmd, int cols) throws SQLException {
		Object f;
		switch (rsmd.getColumnType(cols)) {
			case Types.BIGINT: {
				f = rs.getLong(cols);
				break;
			}
			case Types.INTEGER: {
				f = rs.getInt(cols);
				break;
			}
			case Types.DATE: {
				f = rs.getDate(cols);
				break;
			}
			case Types.FLOAT: {
				f = rs.getFloat(cols);
				break;
			}
			case Types.DOUBLE: {
				f = rs.getDouble(cols);
				break;
			}
			case Types.TIME: {
				f = rs.getDate(cols);
				break;
			}
			case Types.BOOLEAN: {
				f = rs.getBoolean(cols);
				break;
			}
			default: {
				f = rs.getString(cols);
			}
		}
		return f;
	}
}

