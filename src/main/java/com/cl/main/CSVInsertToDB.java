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
 * ��ȡCSV�ļ������뵽���ݿ���
 * @author chenlei
 *
 */
public class CSVInsertToDB {

	private static Logger log = Logger.getLogger(CSVInsertToDB.class);
	
	//�ֶηָ���
	private static final char SEPARATOR = ',';

	//�ַ�����
	private static final String CHARSETNAME = "UTF-8";
	
	public static void main(String[] args) {
		try {
		   //�������
		   String columns = "gsdm,nd,yf,gzrq,pzbm,pzlx,jdbs,zzkm,fp,wb,cz,gnfw,bbje,trprt,hxm";
		   //���Ե�����
		   int ignoreRows = 1;
		   //ÿ�����̴߳��������
		   int batchNum = 320000;
		   //ÿ�����߳�ÿ���ύ������
		   int commitNum = 3000;
		   //����ļ���Ŀ¼
           String filepath = "C:/Users/chenlei/Desktop/������/������/temp/";
           //�γ�File
           File file = new File(filepath);
           //��ȡĿ¼�����е��ļ���
           File[] fileList = file.listFiles();
           
           if(fileList.length==0){
        	   log.info("û����Ҫ������ļ�");
        	   return;
           }
           
           for(File f:fileList){
        	   
        	   String fileName = f.getName();
        	   
        	   String tableName = fileName.substring(0, fileName.indexOf("."));
        	
        	   int count = readCsvAndInsert(filepath, fileName, ignoreRows, tableName, columns, batchNum, commitNum);
        	   
        	   if(DataCheckUtil.dataCheck(count, tableName)){
        		   log.info("+++++++++���ݼ���������ļ������ݿ�������һ��++++++++++");
        	   }else{
        		   log.info("---------���ݼ���������ļ������ݿ���������һ��---------");
        	   }
           }
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			//�ر�����Դ
			JDBCPoolUtil.closeDatasource();
			System.exit(0);
		}
		
	}

	/**
	 * ��ȡÿ��CSV�ļ����������̲߳��뵽���ݿ���
	 * @param path �ļ�·��
	 * @param ignoreRow ������
	 * @param tableName ����ı�
	 * @param columns ��������
	 * @param batchNum ÿ�δ������������
	 * @param commitNum ���߳�ÿ���ύ����������
	 * @throws Exception
	 */
	public static int readCsvAndInsert(String path,String fileName, int ignoreRows, String tableName, String columns, int batchNum, int commitNum)
			throws Exception {
		
		//�������̳߳�8��
		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		
		//�̼߳�����
		int threadCount = 0;
		
		//��¼��ʼʱ��
		Long start = System.currentTimeMillis();

		//����CsvReader���ж�ȡCSV�ļ�
		CsvReader reader = new CsvReader(path + fileName, SEPARATOR, Charset.forName(CHARSETNAME));

		//��ȡ���⣬������
		reader.readHeaders();

		//�м�����
		int count = 0;

		try {
			//����ȡ��CSV��¼���з���List�У�ÿbatchNum��List�ύһ�ζ��̲߳���
			List<String[]> valuesList = new ArrayList<String[]>();

			//���ж�ȡCSV�ļ�
			while (reader.readRecord()) {
				//��ȡÿ�����ݣ��γ�һ��String[]
				String[] line = reader.getValues();

				//����ȡ��CSV��¼���з���List��
				valuesList.add(line);

				//����Ƿ���batchNum��
				if (++count % batchNum == 0) {

					//�̳߳��������߳�
					threadPool.submit(new InsertThread(tableName, columns, new ArrayList<String[]>(valuesList),++threadCount,commitNum));
                    
					log.info("Ŀ���"+tableName+" ������"+ threadCount +"�����߳�,����ִ��"+batchNum+"������");
					
					//���List��������һ�ִ��
					valuesList.clear();
				}

			}
			
			//�̳߳��������߳�
			threadPool.submit(new InsertThread(tableName, columns, valuesList,++threadCount,commitNum));
			
			log.info("Ŀ���"+tableName+ "������"+ threadCount +"�����߳�,����ִ��"+valuesList.size()+"������...");

		} finally {

			//�ر�BufferedReader
			if (reader != null) {
				reader.close();
			}

			//���ú󲻽����µ��߳��������ȴ����������߳�ִ����ϣ��ر��̳߳�
			threadPool.shutdown();

			//�ȴ��̹߳رգ�����
			threadPool.awaitTermination(1, TimeUnit.HOURS);
				
			//��¼����ʱ��
			Long end = System.currentTimeMillis();

			log.info("�ļ�" + path + tableName + "ִ�в��뵽��:"+tableName+" ���,������"+count+"����¼,�ܹ���ʱ:"+(end-start)+"ms,�ٶ�Ϊ:"+count/(end-start)+"��/ms");
		}
		return count;
	}

	
}
