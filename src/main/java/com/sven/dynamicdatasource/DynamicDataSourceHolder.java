/*
 * @(#) DynamicDataSourceHolder.java
 * @Author:wangyi 2016-4-21
 * @Copyright (c) 2002-2016 www.ucsmy.com Limited. All rights reserved.
 */
package com.sven.dynamicdatasource;




public class DynamicDataSourceHolder {
    
    private static final String MASTER = "master";

    private static final String SLAVE = "slave";

    private static final ThreadLocal<String> dataSources = new ThreadLocal<String>();

   /**
     * 设置数据源
     * 
     * @param dataSourceKey
     */
    public static void setDataSource(String dataSourceKey) {
        dataSources.set(dataSourceKey);
    }

    /**
     * 获取数据源
     */
    public static String getDataSource() {
        return (String) dataSources.get();
    }

    /**
     * 标志为master
     */
    public static void setMaster() {
        setDataSource(MASTER);
    }

    /**
     * 标志为slave
     */
    public static void setSlave() {
        setDataSource(SLAVE);
    }


    public static boolean isMaster() {
        return MASTER.equals(getDataSource());
    }

    public static boolean isSlave() {
        return SLAVE.equals(getDataSource());
    }

    /**
     * 清除thread local中的数据源
     */
    public static void clearDataSource() {
        dataSources.remove();
    }
}
