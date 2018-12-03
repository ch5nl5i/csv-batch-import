package com.cl.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ResourceBundle;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

/**
 * 使用DBCP连接池 连接池配置
 * @author chenlei
 *
 */
public class JDBCPoolUtil {

	private static final Logger log = Logger.getLogger(JDBCPoolUtil.class);

	// 创建出BasicDataSource数据源
	private static BasicDataSource datasource = new BasicDataSource();

	private static ResourceBundle rb = ResourceBundle.getBundle("db");

	// 静态代码块，对BasicDataSource类进行配置
	static {
		// 数据库连接信息，必须的
		datasource.setDriverClassName(rb.getString("jdbc.driver"));
		datasource.setUrl(rb.getString("jdbc.url"));
		datasource.setUsername(rb.getString("jdbc.username"));
		datasource.setPassword(rb.getString("jdbc.password"));
		// 对象连接池中的连接数量配置，可选
		// 初始化的连接数
		datasource.setInitialSize(Integer.parseInt(rb.getString("jdbc.initialSize")));
		// 最大的连接数
		datasource.setMaxActive(Integer.parseInt(rb.getString("jdbc.maxActive")));
		// 最大空闲数
		datasource.setMaxIdle(Integer.parseInt(rb.getString("jdbc.maxIdle")));
		// 最小空闲数
		datasource.setMinIdle(Integer.parseInt(rb.getString("jdbc.minIdle")));
	}

	/**
	 * 返回数据源datasource
	 * @return
	 */
	public static DataSource getDataSource() {
		return datasource;
	}

	/**
	 * 执行回滚操作
	 * @param conn
	 */
	public static void rollBack(Connection conn) {
		try {
			conn.rollback();
			log.info("已成功回滚数据库操作");
		} catch (SQLException e) {
			log.info("回滚出现异常");
			e.printStackTrace();
		}
	}

	/**
	 * 执行提交操作
	 * @param conn
	 */
	public static void commit(Connection conn) {
		try {
			conn.commit();
			log.info("本次操作已提交");
		} catch (SQLException e) {
			log.info("数据库提交异常");
			e.printStackTrace();
		}
	}

	/**
	 * 关闭数据源
	 */
	public static void closeDatasource() {
		try {
			datasource.close();
			log.info("成功关闭数据源BasicDataSource");
		} catch (SQLException e) {
			log.info("关闭数据源BasicDataSource失败");
			e.printStackTrace();
		}
	}

	/**
	 * 关闭资源PreparedStatement和Connection
	 * @param conn
	 * @param ps
	 */
	public static void closeConnAndPs(Connection conn, PreparedStatement ps) {

		try {
			if (ps != null) {
				ps.close();
				ps = null;
				log.info("成功关闭PreparedStatement");
			}
			if (conn != null) {
				conn.close();
				conn = null;
				log.info("成功关闭Connection");
			}
			
		} catch (SQLException e) {
			log.error("数据库关闭失败");
		}
	}
}
