/*
 * @(#) DynamicDataSourceTransactionManager.java
 * @Author:wangyi 2016-4-21
 * @Copyright (c) 2002-2016 www.ucsmy.com Limited. All rights reserved.
 */
package com.sven.dynamicdatasource;


import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;

/**
  * 读写分离数据源事务管理器
  * @author Sven Li  
  */
public class DynamicDataSourceTransactionManager extends DataSourceTransactionManager {
	private static final Logger logger = LoggerFactory.getLogger(DynamicDataSourceTransactionManager.class);
    private static final long serialVersionUID = 1L;
    //如果开启事务的时间不超过10S并且出错了，则更换数据源重试
    private  long beginTransactionTimeOut = 10;
    //尝试开启事务的时间
    private static final ThreadLocal<Long>  thread_tran_begin_time = new ThreadLocal<Long>();

    /**
      * 只读事务到从库
      * 读写事务到主库
      */
    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        boolean readOnly = definition.isReadOnly();
        if (readOnly) {
            DynamicDataSourceHolder.setSlave();
        } else {
            DynamicDataSourceHolder.setMaster();
        }
        tryDoBegin(transaction, definition);
        
    }
    /**
     * 	HA算法<br/>
     *  目前通过简单执行时间差判断是否 进行切换数据源重试，如果在设定的过期时间内则可以重试
     * @param transaction
     * @param definition
     * @throws Throwable 
     */
    protected void tryDoBegin(Object transaction, TransactionDefinition definition)  { 
    	//尝试开启事务的时间必须记录在线程变量，如果记录在方法栈，则会无限递归
    	Long beginTime = thread_tran_begin_time.get(); 
    	if(beginTime == null) {
    		beginTime = System.currentTimeMillis();
    		thread_tran_begin_time.set(beginTime);
    	}
    	try{
    		super.doBegin(transaction, definition);
    	}catch (CannotCreateTransactionException ex) {
            long time = System.currentTimeMillis() -beginTime; 
            logger.error("获取连接错误!超时时间"+time/1000,ex);
            DataSource ds =  this.getDataSource();
            if(ds instanceof DynamicMysqlDataSource){
            	//移除不可用的数据源
            	((DynamicMysqlDataSource)ds).removeSlaveDataSource();
            }
            //如果开启事务的时间不超过限制时间并且出错了，则更换数据源重试
            if(time < beginTransactionTimeOut*1000){
            	tryDoBegin(transaction, definition);
    		}else{
    			throw ex;
    		}
    	}finally{
    		thread_tran_begin_time.remove();
    		DynamicMysqlDataSource.cleanLocalDataSource();
    	}
    }

    /**
      * 不需要事务到从库
      * 需要事务到主库
      */
    @Override
    protected boolean isExistingTransaction(Object transaction) {
        boolean isExisting = super.isExistingTransaction(transaction);
        //如果不存在事务则使用主库数据源 
        if (!isExisting){
            DynamicDataSourceHolder.setMaster();
        }
        return isExisting;
    }

    /**
      * 
      * 清理本地线程的数据源
      */
    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        super.doCleanupAfterCompletion(transaction);
        DynamicDataSourceHolder.clearDataSource();
    }
	public long getBeginTransactionTimeOut() {
		return beginTransactionTimeOut;
	}
	public void setBeginTransactionTimeOut(long beginTransactionTimeOut) {
		this.beginTransactionTimeOut = beginTransactionTimeOut;
	}
    
    

}
