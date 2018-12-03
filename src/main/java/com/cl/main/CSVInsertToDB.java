package com.cl.main;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.csvreader.CsvReader;
import com.cl.thread.InsertThread;
import com.cl.util.DataCheckUtil;
import com.cl.util.JDBCPoolUtil;

/**
 * 读取CSV文件并插入到数据库中
 * @author chenlei
 *
 */
public class CSVInsertToDB {

	private static Logger log = Logger.getLogger(CSVInsertToDB.class);
	
	//字段分隔符
	private static final char SEPARATOR = ',';

	//字符编码
	private static final String CHARSETNAME = "UTF-8";
	
	public static void main(String[] args) {
		try {
		   //插入的列
		   String columns = "gsdm,nd,yf,gzrq,pzbm,pzlx,jdbs,zzkm,fp,wb,cz,gnfw,bbje,trprt,hxm";
		   //忽略的行数
		   int ignoreRows = 1;
		   //每个子线程处理的数量
		   int batchNum = 320000;
		   //每个子线程每次提交的数量
		   int commitNum = 3000;
		   //存放文件的目录
           String filepath = "C:/Users/chenlei/Desktop/给陈磊/给陈磊/temp/";
           //形成File
           File file = new File(filepath);
           //获取目录下所有的文件名
           File[] fileList = file.listFiles();
           
           if(fileList.length==0){
        	   log.info("没有需要处理的文件");
        	   return;
           }
           
           for(File f:fileList){
        	   
        	   String fileName = f.getName();
        	   
        	   String tableName = fileName.substring(0, fileName.indexOf("."));
        	
        	   int count = readCsvAndInsert(filepath, fileName, ignoreRows, tableName, columns, batchNum, commitNum);
        	   
        	   if(DataCheckUtil.dataCheck(count, tableName)){
        		   log.info("+++++++++数据检查完整，文件和数据库中条数一致++++++++++");
        	   }else{
        		   log.info("---------数据检查完整，文件和数据库中条数不一致---------");
        	   }
           }
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			//关闭数据源
			JDBCPoolUtil.closeDatasource();
			System.exit(0);
		}
		
	}

	/**
	 * 读取每个CSV文件并启动多线程插入到数据库中
	 * @param path 文件路径
	 * @param ignoreRow 标题行
	 * @param tableName 插入的表
	 * @param columns 插入表的列
	 * @param batchNum 每次处理的数据条数
	 * @param commitNum 子线程每次提交的数据条数
	 * @throws Exception
	 */
	public static int readCsvAndInsert(String path,String fileName, int ignoreRows, String tableName, String columns, int batchNum, int commitNum)
			throws Exception {
		
		//定长的线程池8个
		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		
		//线程计数器
		int threadCount = 0;
		
		//记录开始时间
		Long start = System.currentTimeMillis();

		//利用CsvReader逐行读取CSV文件
		CsvReader reader = new CsvReader(path + fileName, SEPARATOR, Charset.forName(CHARSETNAME));

		//读取标题，并忽略
		reader.readHeaders();

		//行计数器
		int count = 0;

		try {
			//将读取的CSV记录逐行放入List中，每batchNum行List提交一次多线程操作
			List<String[]> valuesList = new ArrayList<String[]>();

			//逐行读取CSV文件
			while (reader.readRecord()) {
				//读取每行数据，形成一个String[]
				String[] line = reader.getValues();

				//将读取的CSV记录逐行放入List中
				valuesList.add(line);

				//检查是否满batchNum行
				if (++count % batchNum == 0) {

					//线程池启动多线程
					threadPool.submit(new InsertThread(tableName, columns, new ArrayList<String[]>(valuesList),++threadCount,commitNum));
                    
					log.info("目标表："+tableName+" 开启第"+ threadCount +"个子线程,本次执行"+batchNum+"条数据");
					
					//清空List，开启新一轮存放
					valuesList.clear();
				}

			}
			
			//线程池启动多线程
			threadPool.submit(new InsertThread(tableName, columns, valuesList,++threadCount,commitNum));
			
			log.info("目标表："+tableName+ "开启第"+ threadCount +"个子线程,本次执行"+valuesList.size()+"条数据...");

		} finally {

			//关闭BufferedReader
			if (reader != null) {
				reader.close();
			}

			//调用后不接受新的线程启动，等待所有其他线程执行完毕，关闭线程池
			threadPool.shutdown();

			//等待线程关闭，阻塞
			threadPool.awaitTermination(1, TimeUnit.HOURS);
				
			//记录结束时间
			Long end = System.currentTimeMillis();

			log.info("文件" + path + tableName + "执行插入到表:"+tableName+" 完毕,共插入"+count+"条记录,总共用时:"+(end-start)+"ms,速度为:"+count/(end-start)+"行/ms");
		}
		return count;
	}

	
}
