package com.cl.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 数据检查工具类
 * @author chenlei
 *
 */
public class DataCheckUtil {

	/**
	 * 检查数量是否与文件中的记录数一致
	 * @param count
	 * @param tableName
	 * @return
	 */
	public static boolean dataCheck(int count, String tableName) {
		
		if(count==0 || tableName==null || "".equals(tableName)){
			return false;
		}

		if (count == getCount(tableName)) {
			return true;
		}

		return false;
	}

	/**
	 * 获取表中的总记录数
	 * @param tableName
	 * @return
	 */
	private static int getCount(String tableName) {

		int count = 0;

		Connection conn = null;
		try {
			conn = JDBCPoolUtil.getDataSource().getConnection();
		} catch (SQLException e1) {
			e1.printStackTrace();
			return count;
		}

		String sql = "select count(1) count from " + tableName;
		PreparedStatement pstmt = null;

		try {
			pstmt = (PreparedStatement) conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				count = Integer.valueOf(rs.getString("count"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			count = 0;
		}
		return count;
	}
	
}
