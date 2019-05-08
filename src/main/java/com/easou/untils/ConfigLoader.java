package com.easou.untils;

import com.easou.Constants.UserConstants;

import java.util.Properties;

/**
 * ClassName ConfigLoader
 * 功能: 配置文件加载类
 * Author yangweifeng
 * Date 2018/9/13 16:48
 * Version 1.0
 **/
public final class ConfigLoader {
	private static Properties props = new Properties();
	// 独立配置文件
	static {
		props.setProperty(UserConstants.APPLICATION_NAME,"Channel_Promotion_Count_Minute");
		props.setProperty(UserConstants.BOOTSTRAP_SERVERS,"*:9092,*:9092,*:9092,*:9092,*:9092,*:9092");
		props.setProperty(UserConstants.ZOOKEEPER_SERVERS,"*:2181,*:2181,*:2181");
		props.setProperty(UserConstants.GROUPID,"Channel_Promotion_Count_Minute");
		props.setProperty(UserConstants.KAFKA_TOPIC,"mobile_info");
		props.setProperty(UserConstants.REDIS_IP,"*");
		props.setProperty(UserConstants.REDIS_PORT,"6379");
		props.setProperty(UserConstants.TIME_INTERVAL,"10");
		props.setProperty(UserConstants.SPARK_MINUTE_OUTPUT,"...");
		props.setProperty(UserConstants.SPARK_HOUR_OUTPUT,"...");
	}
	/**
	 * 根据Properties中的key获取value
	 * 
	 * @param key      键
	 * @return value   值
	 */
	public synchronized static String getProperties(String key) {
		return props.getProperty(key);
	}
	/**
	 * 根据Properties中的key获取value
	 *
	 * @param key      键
	 * @return value   值
	 */
	public synchronized static Integer getIntegerProperties(String key) {
		return Integer.parseInt(props.getProperty(key));
	}

	public static void main(String[] args) {
		System.out.println(ConfigLoader.getIntegerProperties(UserConstants.TIME_INTERVAL));
	}
}