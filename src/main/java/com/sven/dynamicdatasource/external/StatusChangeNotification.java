package com.sven.dynamicdatasource.external;

/**
 * 从库状态变更通知
 * @author Sven Li 
 *
 */
public interface StatusChangeNotification {
	void offlineNoti(String configKey,String message);
	void online(String configKey);
}
