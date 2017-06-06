package com.sven.dynamicdatasource.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/*
 * 检查网络地址及端口号是否可用
 */
public class ConnUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(ConnUtil.class);
	
	/**
	 * 
	 * @param host 地址
	 * @param port 端口号 
	 * @param timeOut 超时时间(毫秒)
	 * @return
	 */
	public static boolean isReachable(String host,int port,int timeOut){
		boolean status = false;
		//最大重试3次socket连接
		for(int i=1;i<4;i++){
			Socket socket = new Socket();
			SocketAddress address = new InetSocketAddress(host, port);
			try {
				socket.connect(address, timeOut*1000);
				status = socket.isConnected();
			} catch (IOException e) {
				logger.error("isReachable==="+host+":"+port+"============times"+i, e);
			}finally{
				try {
					socket.close();
				} catch (IOException e) {
					logger.error("isReachable==="+host+":"+port+"============times"+i, e);
				}
			}
			if(status){
				break;
			}
		}
		return status;
	}
	/**
	 * 
	 * @param hostAndPort 目标地址 ip:port
	 * @param timeOutSeconds 超时时间 秒
	 * @return
	 */
	public static boolean isReachable(String hostAndPort, int timeOutSeconds) {
		try {
			String[] strings = hostAndPort.split(":");
			return isReachable(strings[0], Integer.valueOf(strings[1]), timeOutSeconds*1000);
		} catch (Exception e) {
			return false;
		}
	} 
 
	
}
