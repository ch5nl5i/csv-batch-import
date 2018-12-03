package com.cl.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ResourceBundle;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

/**
 * ʹ��DBCP���ӳ� ���ӳ�����
 * @author chenlei
 *
 */
public class JDBCPoolUtil {

	private static final Logger log = Logger.getLogger(JDBCPoolUtil.class);

	// ������BasicDataSource����Դ
	private static BasicDataSource datasource = new BasicDataSource();

	private static ResourceBundle rb = ResourceBundle.getBundle("db");

	// ��̬����飬��BasicDataSource���������
	static {
		// ���ݿ�������Ϣ�������
		datasource.setDriverClassName(rb.getString("jdbc.driver"));
		datasource.setUrl(rb.getString("jdbc.url"));
		datasource.setUsername(rb.getString("jdbc.username"));
		datasource.setPassword(rb.getString("jdbc.password"));
		// �������ӳ��е������������ã���ѡ
		// ��ʼ����������
		datasource.setInitialSize(Integer.parseInt(rb.getString("jdbc.initialSize")));
		// ����������
		datasource.setMaxActive(Integer.parseInt(rb.getString("jdbc.maxActive")));
		// ��������
		datasource.setMaxIdle(Integer.parseInt(rb.getString("jdbc.maxIdle")));
		// ��С������
		datasource.setMinIdle(Integer.parseInt(rb.getString("jdbc.minIdle")));
	}

	/**
	 * ��������Դdatasource
	 * @return
	 */
	public static DataSource getDataSource() {
		return datasource;
	}

	/**
	 * ִ�лع�����
	 * @param conn
	 */
	public static void rollBack(Connection conn) {
		try {
			conn.rollback();
			log.info("�ѳɹ��ع����ݿ����");
		} catch (SQLException e) {
			log.info("�ع������쳣");
			e.printStackTrace();
		}
	}

	/**
	 * ִ���ύ����
	 * @param conn
	 */
	public static void commit(Connection conn) {
		try {
			conn.commit();
			log.info("���β������ύ");
		} catch (SQLException e) {
			log.info("���ݿ��ύ�쳣");
			e.printStackTrace();
		}
	}

	/**
	 * �ر�����Դ
	 */
	public static void closeDatasource() {
		try {
			datasource.close();
			log.info("�ɹ��ر�����ԴBasicDataSource");
		} catch (SQLException e) {
			log.info("�ر�����ԴBasicDataSourceʧ��");
			e.printStackTrace();
		}
	}

	/**
	 * �ر���ԴPreparedStatement��Connection
	 * @param conn
	 * @param ps
	 */
	public static void closeConnAndPs(Connection conn, PreparedStatement ps) {

		try {
			if (ps != null) {
				ps.close();
				ps = null;
				log.info("�ɹ��ر�PreparedStatement");
			}
			if (conn != null) {
				conn.close();
				conn = null;
				log.info("�ɹ��ر�Connection");
			}
			
		} catch (SQLException e) {
			log.error("���ݿ�ر�ʧ��");
		}
	}
}
