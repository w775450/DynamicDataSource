/*
 * @(#) DynamicDataSource.java
 * @Author:wangyi 2016-4-21
 * @Copyright (c) 2002-2016 www.ucsmy.com Limited. All rights reserved.
 */
package com.sven.dynamicdatasource;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import com.sven.dynamicdatasource.external.StatusChangeNotification;
import com.sven.dynamicdatasource.util.ConnUtil;
 

/**
 * 动态数据源
 * @author Sven Li
 */
public class DynamicMysqlDataSource extends AbstractRoutingDataSource {
	

	private static final Logger logger = LoggerFactory.getLogger(DynamicMysqlDataSource.class);
	/**
	 * mysql心跳检测从库IO和SQL转态的语句
	 */
	private static final String validationSlaveQuery = "show slave status";
	
	//检查心跳的超时时间(默认5秒)
	private Integer validationQueryTimeout = 5;
	//主库数据源
	private DataSource master; 
	/*
	 * 所有从库数据源对象的持有者 ,spring初始化本类的时候填充这个map.其他的容器都是从这里copy出去的
	 * 这个key也是用户通知的标识
	 */
	private Map<String, DataSource> allSlaveMap;
	//状态变更通知，可选注入
	private StatusChangeNotification changeNotification;
	
	
	
 
	private List<DataSource> normalSlaves;
	private List<DataSource> abnormalSlaves;  
	// 有效的从库数据源队列，与normalSlaves同步，<strong>会自动移除normalSlaves不存在的元素，但是添加元素的时候需要额外添加到normalSlaves</strong>
	private Queue<DataSource> slavesQueue;
	private ReentrantLock lock = new ReentrantLock();
	private Timer  timer;
	private Timer  timer1;

	//当前使用的数据源,用来做外部下线
	private static ThreadLocal<DataSource> localDataSource = new ThreadLocal<DataSource>();

	@Override
	protected Object determineCurrentLookupKey() {
		return null;
	}

	@Override
	public void afterPropertiesSet() {
		/*
		 * 初始化容器填充数据 start
		 */
		normalSlaves = new ArrayList<DataSource>(); 
		for(String key : allSlaveMap.keySet()){
			normalSlaves.add(allSlaveMap.get(key));
		}
		slavesQueue = new ConcurrentLinkedQueue<DataSource>();
		abnormalSlaves = new ArrayList<DataSource>();
		for (DataSource ds : normalSlaves) {
			slavesQueue.add(ds); 
		} 
		/*
		 * 初始化容器填充数据 end
		 */
		
		/*
		 * 启动检测线程
		 */
		timer = new Timer();
		timer1 = new Timer();
		//10s后启动检测线程，每隔2秒执行一次 下线有较强的即时要求
		timer.schedule(new CheckNormalSlavesTask(), 10000, 2000);
		//11s后启动检测线程，每隔10秒执行一次 上线没有即时要求
		timer1.schedule(new CheckAbNormalSlavesTask(), 11000, 10000);
	}


	/**
	 * 根据标识 获取数据源
	 */
	@Override
	protected DataSource determineTargetDataSource() {
		DataSource returnDataSource = null;
		if (DynamicDataSourceHolder.isSlave()) {
			//从列头移动到列尾,这里加锁防止queue被拿空
			synchronized (DynamicMysqlDataSource.class) {
				do {
					returnDataSource = slavesQueue.poll();
				} while (returnDataSource != null && !normalSlaves.contains(returnDataSource));  
				if(returnDataSource  != null){
					logger.info("isSlave============================true");
					slavesQueue.offer(returnDataSource); 
				}else{	//当所有的从库都不可用时使用主库
					logger.info("isSlave============================false");
					returnDataSource = master;
				}
			}
		} else {
			logger.info("isSlave============================false");
			returnDataSource = master;
		}
		localDataSource.set(returnDataSource);
		return returnDataSource;
	}
	
	/**
	 * 外部手动下线当前线程使用的数据源 （运行过程中）
	 * @param slave
	 */
	public  void removeSlaveDataSource() {
		if (DynamicDataSourceHolder.isSlave()) {
			DataSource slave = localDataSource.get();
			//这里是针对所有从库都不可用的时候使用主库读取时，主库不需要加入abnormalSlaves
			if(slave == master){
				return;
			}
			offline(slave,"使用数据源的时候发生错误，外部下线1");
		}
	}
	/**
	 * 线程安全操作   下线数据源
	 * @param slave
	 */
	protected void offline(DataSource slave,String message) {
		lock.lock();
		try {
			normalSlaves.remove(slave); 
			//如果不存在与不正常列表才添加
			if(!abnormalSlaves.contains(slave)) abnormalSlaves.add(slave);
			if(changeNotification != null) changeNotification.offlineNoti(getMapKey(slave), message);
		} catch (Exception e) {
			logger.info("移除实现数据源出现问题！", e);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * 线程安全的操作   上线数据源
	 * @param slave
	 */
	protected void online(DataSource ds) {
		lock.lock();
		try{
			normalSlaves.add(ds);
			slavesQueue.offer(ds); 
			abnormalSlaves.remove(ds); 
			if(changeNotification != null) changeNotification.online(getMapKey(ds));
		}finally{
			lock.unlock();
		}
	}
	 
 
	 
	class CheckNormalSlavesTask extends TimerTask {   
	    @Override  
	    public void run() {
	    	try{ 
	    		checkNormalSlaves();
	    	}catch (Exception e) {
	    		//ignore
			}
	    }  
	  
	} 
	
	 
	class CheckAbNormalSlavesTask extends TimerTask {   
	    @Override  
	    public void run() {
	    	try{
	    		checkAbnormalSlaves();
	    	}catch (Exception e) {
	    		//ignore
			}
	    
	    }  
	  
	} 
	
	/**
	 * 自动上线可用数据源<br/>
	 * 检查AbnormalSlaves，如果正常则丢回normalSlaves 
	 */
	protected void checkAbnormalSlaves(){
		//循环检查所有的失效从库数据源
		for(int i = 0; i < abnormalSlaves.size();i++){ 
			DataSource ds = null;
			Connection conn = null;
			try {
				ds = abnormalSlaves.get(i);
				//step检测可连接性
				String ipAndPort = getMapKey(ds);
				if(ConnUtil.isReachable(ipAndPort, validationQueryTimeout)){
					//step：  数据库直连检测
					conn = ds.getConnection(); 
					validateSlaveStatus(conn);
					online(ds);
				}
				break;
			} catch (Throwable e) { 
				continue;
			}finally{
				if(conn != null && ds != null) DataSourceUtils.releaseConnection(conn, ds);
			} 
		}
	}


	//获取数据源的id
	private String getMapKey(DataSource ds){
		
		for(String key : allSlaveMap.keySet()){
			//这里如果是同一个引用则返回当前的key
			if(allSlaveMap.get(key) == ds){
				 return key;
			}
		}
		return null;
	}
	
	/**
	 * 自动下线无效数据源<br/>
	 * 检查snormalSlaves，如果不正常则丢到AbnormalSlave
	 */
	protected void checkNormalSlaves(){
		//循环检查所有的失效从库数据源
		for(int i = 0; i < normalSlaves.size();i++){
			DataSource ds = null;
			Connection conn = null;
			try {
				ds = normalSlaves.get(i);
				//step检测可连接性
				String ipAndPort = getMapKey(ds);
				if(!ConnUtil.isReachable(ipAndPort, validationQueryTimeout)){
					throw new Exception("不能连接到"+ipAndPort);
				}
				conn = ds.getConnection();
				validateSlaveStatus(conn);
			} catch (Throwable e) {
				logger.error("数据源检查线程发现不可用的数据源",e);
				offline(ds,e.getMessage());
				break;
			}finally{
				if(conn != null && ds != null) DataSourceUtils.releaseConnection(conn, ds);
			} 
		}
	}
	 
	/**
	 * 检查从节点同步复制状态
	 * @param conn
	 * @throws SQLException
	 */
	private void validateSlaveStatus(Connection conn) throws SQLException {
        String query = validationSlaveQuery;
        if(conn.isClosed()) {
            throw new SQLException("validateConnection: connection closed");
        }
        if(null != query) {
            Statement stmt = null;
            ResultSet rset = null;
            try {
                stmt = conn.createStatement();
                if (validationQueryTimeout > 0) {
                    stmt.setQueryTimeout(validationQueryTimeout);
                }
                rset = stmt.executeQuery(query);
                if(!rset.next()) {
                    throw new SQLException("validationQuery didn't return a row");
                }
                if(!"Yes".equals(rset.getString("Slave_IO_Running"))){
                	logger.debug(conn.getMetaData().getURL()+"  Slave_IO_Running is No");
                	throw new SQLException("Slave_IO_Running is No");
                }
                if(!"Yes".equals(rset.getString("Slave_SQL_Running"))){
                	logger.debug(conn.getMetaData().getURL()+"  Slave_SQL_Running is No");
                	throw new SQLException("Slave_SQL_Running is No");
                }
                Integer timeBehindMaster = rset.getInt("Seconds_Behind_Master");
                if(timeBehindMaster > 10){
                	logger.debug(conn.getMetaData().getURL()+"  Seconds_Behind_Master is to long ! seconds is "+timeBehindMaster);
                	throw new SQLException("Seconds_Behind_Master is to long!  seconds is "+timeBehindMaster);
                }
                
            } finally {
                if (rset != null) {
                    try {
                        rset.close();
                    } catch(Exception t) {
                        // 忽略
                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch(Exception t) {
                    	 // 忽略
                    }
                }
            }
        }
	}

	/**
	 * 删除线程变量
	 */
	public static void cleanLocalDataSource() {
		localDataSource.remove();
	}
	
	/**
	 * 容器销毁时关闭线程
	 */
	public void onDetory(){
		timer.cancel();
		timer1.cancel();
	}

	public DataSource getMaster() {
		return master;
	}

	public void setMaster(DataSource master) {
		this.master = master;
	}

	public List<DataSource> getSlaves() {
		return normalSlaves;
	}

	public void setSlaves(List<DataSource> normalSlaves) {
		this.normalSlaves = normalSlaves;
	}
	

 
	public Integer getValidationQueryTimeout() {
		return validationQueryTimeout;
	}

	public void setValidationQueryTimeout(Integer validationQueryTimeout) {
		this.validationQueryTimeout = validationQueryTimeout;
	}

	public  Map<String, DataSource> getAllSlaveMap() {
		return allSlaveMap;
	}

	public void setAllSlaveMap(Map<String, DataSource> allSlaveMap) {
		 this.allSlaveMap = allSlaveMap;
	}

	
		

}
