package com.easou.untils;
import com.easou.Constants.UserConstants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;

/**
 * ClassName RedisUntil
 * 功能: redis连接处理类 普通方式连接
 * Author yangweifeng
 * Date 2018/9/14 10:07
 * Version 1.0
 **/
public class RedisUntil implements Serializable {

    private static JedisPool pool = null;
    /**
     * 构建redis连接池
     * return JedisPool
     * Author yangweifeng
     */
    public synchronized static JedisPool getJedisPool() throws Exception{
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            // 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
            // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
            //config.setMaxActive(5);
            config.setMaxTotal(5);
            // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            config.setMaxIdle(2);
            // 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
            config.setMaxWaitMillis(1000 * 100);
            // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
            config.setTestOnBorrow(true);
            pool = new JedisPool(config, ConfigLoader.getProperties(UserConstants.REDIS_IP), ConfigLoader.getIntegerProperties(UserConstants.REDIS_PORT));
        }
        return pool;
    }

    /**
     * 判断是否存在指定的key
     * @param key 键
     * @return true false
     * @Author yangweifeng
     */
    public static Boolean isExit(String key) throws Exception{
        JedisPool jedisPool = RedisUntil.getJedisPool();
        Jedis jedis = jedisPool.getResource();
        jedis.select(UserConstants.REDIS_NUMBER);
        Boolean is1 = jedis.sismember(UserConstants.REDIS_SET_KEY, key);
        if (!is1){
            jedis.sadd(UserConstants.REDIS_SET_KEY,key);
        }
        jedis.close();
        return is1;
    }

    public static void isExitTest() throws Exception{
        JedisPool jedisPool = RedisUntil.getJedisPool();
        Jedis jedis = jedisPool.getResource();
        jedis.select(UserConstants.REDIS_NUMBER);
        Long time1 = System.currentTimeMillis();
        Boolean is1 = jedis.sismember(UserConstants.REDIS_SET_KEY,"fffa0025727badbb421bab38d9e1f9bd38861f89,1002");
        Long time2 = System.currentTimeMillis();
        System.out.println(time2 - time1);
        System.out.println(is1);
        Long set = jedis.scard(UserConstants.REDIS_SET_KEY);
        System.out.println(set);
        jedis.close();
    }
    public static void main(String[] args) throws Exception {
       // RedisUntil.isExitTest();
    }
}