package com.cl.thread;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
 
import org.apache.log4j.Logger;

import com.cl.util.JDBCPoolUtil; 
 
/**
 * 数据库操作线程类
 * @author Administrator
 *
 */
public class InsertThread implements Runnable{
 
	private static Logger log = Logger.getLogger(InsertThread.class);
	
	//表名
	private String table = null;
	//列名
	private String columns = null;
	//本次需插入的csv记录
	private List<String[]> valuesList = null;
	//子线程标识
	private int threadCount = 0;
	//每次提交的记录数
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
		
		//声明一个数据库连接
		Connection conn = null;
		
		try {
			//从数据源获取连接
			conn = JDBCPoolUtil.getDataSource().getConnection();
			
			// 关闭事物自动提交
			conn.setAutoCommit(false);
			
		} catch (SQLException e1) {
			e1.printStackTrace();
			log.info("目标表："+table+" 第"+threadCount+"个子线程,获取数据库连接异常,直接返回");
			return;
		}
		
		//声明一个PreparedStatement
		PreparedStatement ps = null;
		
		//本次插入的条数
		int size = valuesList.size();
		
		//根据表名和列名拼装SQL
	    String sql = getSql(table,columns);
	    
		try {	
			//实例化PreparedStatement
			ps = conn.prepareStatement(sql);
			
			//行计数器
			int count = 0;
			
			//本次执行插入的开始时间
			Long begin = System.currentTimeMillis();
			
			//检查
			if (valuesList != null && size > 0) {
				
				//列索引
				int k = 1;
				
				//针对一条数据，解析每个字段，按顺序放入PreparedStatement中
				for (String[] value : valuesList) {
					
					for (String v : value) {
						//只针对插入列为字符串的情形，如有其它类型需分别set，如插入int型，ps.setInt(k++, v);
						ps.setObject(k++, v);
					}
					
					//一行结束，列索引还原
					k = 1;
					
					//将PreparedStatement添加到批处理中
					ps.addBatch();
					
					//满3000行处理一次
					if (++count % commitNum == 0) {
						//发送批处理命令到数据库
						ps.executeBatch();
						//执行提交
						JDBCPoolUtil.commit(conn);
						//清空批处理
						ps.clearBatch();
						log.info("目标表："+table+" 第"+threadCount+"个子线程,满"+commitNum+"行提交一次,已处理"+count+"行,剩余"+(size-count)+"行");
					}
				}
			}
			
			//释放资源
			valuesList = null;
			
			//发送批处理命令到数据库
			ps.executeBatch();
			
			//执行提交
			JDBCPoolUtil.commit(conn);
			log.info("目标表："+table+" 第"+threadCount+"个子线程,最后一次提交,已处理"+count+"行,剩余"+(size-count)+"行");

			//本次执行插入的结束时间
			Long end = System.currentTimeMillis();
			
			log.info("目标表："+table+" 第"+threadCount+"个子线程提交完成,共处理"+size+"条记录,共耗时:"+(end - begin)+"ms");
			
			return;
		} catch (SQLException e) {
			
			//出现异常进行回滚操作
			JDBCPoolUtil.rollBack(conn);
			
			log.error("目标表："+table+" 第"+threadCount+"个子线程提交异常");
		} finally {
			
			try {
				//清空批处理
				ps.clearBatch();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			//关闭连接(关闭PreparedStatement和Connection)
			JDBCPoolUtil.closeConnAndPs(conn, ps);
		}
		//如有异常，则返回
		return;
	}
 
 
	/**
	 * 根据表名和列名拼装SQL
	 * @param table 表名
	 * @param columns 列名
	 * @return
	 */
	private String getSql(String table, String columns) {
		
        StringBuffer sqlSbf = new StringBuffer();
		
		//拼接表名和列名
		sqlSbf.append("INSERT INTO ").append(table).append("(").append(columns).append(") VALUES (");
		
		//检查有多少个字段
		int columnsCount = columns.split(",").length;
		
		//拼接占位符
		for(int i=0;i<columnsCount;i++) {
			
			//最后一个不加逗号，拼上右括号
			if(i == columnsCount - 1) {
				
				sqlSbf.append("?)");
				break;
			}
			
			//拼接问号
			sqlSbf.append("?,");
		}
		
		return sqlSbf.toString();
	}

}
