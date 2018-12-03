package com.cl.thread;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
 
import org.apache.log4j.Logger;

import com.cl.util.JDBCPoolUtil; 
 
/**
 * ���ݿ�����߳���
 * @author Administrator
 *
 */
public class InsertThread implements Runnable{
 
	private static Logger log = Logger.getLogger(InsertThread.class);
	
	//����
	private String table = null;
	//����
	private String columns = null;
	//����������csv��¼
	private List<String[]> valuesList = null;
	//���̱߳�ʶ
	private int threadCount = 0;
	//ÿ���ύ�ļ�¼��
	private int commitNum = 0;
	
	public InsertThread(String table, String columns, List<String[]> valuesList,int threadCount,int commitNum) {
		this.table = table;
		this.columns = columns;
		this.valuesList = valuesList;
		this.threadCount = threadCount;
		this.commitNum = commitNum;
	}

	@Override
	public void run() {
		insert();
	}
	
	private void insert(){
		
		//����һ�����ݿ�����
		Connection conn = null;
		
		try {
			//������Դ��ȡ����
			conn = JDBCPoolUtil.getDataSource().getConnection();
			
			// �ر������Զ��ύ
			conn.setAutoCommit(false);
			
		} catch (SQLException e1) {
			e1.printStackTrace();
			log.info("Ŀ���"+table+" ��"+threadCount+"�����߳�,��ȡ���ݿ������쳣,ֱ�ӷ���");
			return;
		}
		
		//����һ��PreparedStatement
		PreparedStatement ps = null;
		
		//���β��������
		int size = valuesList.size();
		
		//���ݱ���������ƴװSQL
	    String sql = getSql(table,columns);
	    
		try {	
			//ʵ����PreparedStatement
			ps = conn.prepareStatement(sql);
			
			//�м�����
			int count = 0;
			
			//����ִ�в���Ŀ�ʼʱ��
			Long begin = System.currentTimeMillis();
			
			//���
			if (valuesList != null && size > 0) {
				
				//������
				int k = 1;
				
				//���һ�����ݣ�����ÿ���ֶΣ���˳�����PreparedStatement��
				for (String[] value : valuesList) {
					
					for (String v : value) {
						//ֻ��Բ�����Ϊ�ַ��������Σ���������������ֱ�set�������int�ͣ�ps.setInt(k++, v);
						ps.setObject(k++, v);
					}
					
					//һ�н�������������ԭ
					k = 1;
					
					//��PreparedStatement��ӵ���������
					ps.addBatch();
					
					//��3000�д���һ��
					if (++count % commitNum == 0) {
						//����������������ݿ�
						ps.executeBatch();
						//ִ���ύ
						JDBCPoolUtil.commit(conn);
						//���������
						ps.clearBatch();
						log.info("Ŀ���"+table+" ��"+threadCount+"�����߳�,��"+commitNum+"���ύһ��,�Ѵ���"+count+"��,ʣ��"+(size-count)+"��");
					}
				}
			}
			
			//�ͷ���Դ
			valuesList = null;
			
			//����������������ݿ�
			ps.executeBatch();
			
			//ִ���ύ
			JDBCPoolUtil.commit(conn);
			log.info("Ŀ���"+table+" ��"+threadCount+"�����߳�,���һ���ύ,�Ѵ���"+count+"��,ʣ��"+(size-count)+"��");

			//����ִ�в���Ľ���ʱ��
			Long end = System.currentTimeMillis();
			
			log.info("Ŀ���"+table+" ��"+threadCount+"�����߳��ύ���,������"+size+"����¼,����ʱ:"+(end - begin)+"ms");
			
			return;
		} catch (SQLException e) {
			
			//�����쳣���лع�����
			JDBCPoolUtil.rollBack(conn);
			
			log.error("Ŀ���"+table+" ��"+threadCount+"�����߳��ύ�쳣");
		} finally {
			
			try {
				//���������
				ps.clearBatch();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			//�ر�����(�ر�PreparedStatement��Connection)
			JDBCPoolUtil.closeConnAndPs(conn, ps);
		}
		//�����쳣���򷵻�
		return;
	}
 
 
	/**
	 * ���ݱ���������ƴװSQL
	 * @param table ����
	 * @param columns ����
	 * @return
	 */
	private String getSql(String table, String columns) {
		
        StringBuffer sqlSbf = new StringBuffer();
		
		//ƴ�ӱ���������
		sqlSbf.append("INSERT INTO ").append(table).append("(").append(columns).append(") VALUES (");
		
		//����ж��ٸ��ֶ�
		int columnsCount = columns.split(",").length;
		
		//ƴ��ռλ��
		for(int i=0;i<columnsCount;i++) {
			
			//���һ�����Ӷ��ţ�ƴ��������
			if(i == columnsCount - 1) {
				
				sqlSbf.append("?)");
				break;
			}
			
			//ƴ���ʺ�
			sqlSbf.append("?,");
		}
		
		return sqlSbf.toString();
	}

}
