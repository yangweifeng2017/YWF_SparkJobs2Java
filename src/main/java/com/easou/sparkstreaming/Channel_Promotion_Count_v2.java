package com.easou.sparkstreaming;

import com.alibaba.fastjson.JSONObject;
import com.easou.Constants.UserConstants;
import com.easou.common.AbstractCommonHelper;
import com.easou.interfaces.BIModel;
import com.easou.spark.userrddmultipleyextoutputformat.RDDMultipleTextOutputFormat;
import com.easou.untils.ConfigLoader;
import com.easou.untils.DateUntil;
import com.easou.untils.RedisUntil;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.io.NullWritable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * ClassName Channel_Promotion_Count_v41
 * 功能: mobile_info 日志 按分钟每个渠道新增用户数
 * 运行方式与参数: Channel_Promotion_Count_v41
 * Author yangweifeng
 * Date 2018/11/20 10:47
 * Version 1.0
 **/
public class Channel_Promotion_Count_v2 extends AbstractCommonHelper implements BIModel {
    private static final long serialVersionUID = 1L;
    @Override
    public void execute()  {
        SparkConf sparkConf = new SparkConf().setAppName(ConfigLoader.getProperties(UserConstants.APPLICATION_NAME)).setMaster("local[1]");
      //  SparkConf sparkConf = new SparkConf().setAppName(ConfigLoader.getProperties(UserConstants.APPLICATION_NAME));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.minutes(ConfigLoader.getIntegerProperties(UserConstants.TIME_INTERVAL)));
        // 设置要读取的主题
        Set<String> topicsSet = new HashSet<>();
        topicsSet.add(ConfigLoader.getProperties(UserConstants.KAFKA_TOPIC));
        // 设置连接参数
        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigLoader.getProperties(UserConstants.BOOTSTRAP_SERVERS));
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigLoader.getProperties(UserConstants.GROUPID));
        JavaPairInputDStream<String,String> data = KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
        // 数据处理
        final JavaDStream<String> lines = data.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) {
                return stringStringTuple2._2;
            }
        });
        lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String value) {
                List<String> listDate =  DateUntil.getDateOneMinuteAgo();
                StringBuffer key = new StringBuffer();
                try{
                    JSONObject line = JSONObject.parseObject(value);
                    String phone_udid = line.getString(UserConstants.PHONE_UDID);
                    String phone_softversion = line.getString(UserConstants.PHONE_SOFTVERSION);
                    String app = line.getString(UserConstants.APPKEY);
                    String app_name = app;
                    String cpid = line.getString(UserConstants.CPID);
                    key.append(app).append(",").append(app_name).append(",").append(cpid).append(",").append(phone_softversion).append(",").append(listDate.get(1)).append(",").append(listDate.get(0));
                    if (!RedisUntil.isExit(phone_udid + "," + app)){
                        return new Tuple2<>(key.toString(),1);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    return new Tuple2<>("redisException:" + key.toString(),0);
                }
                return new Tuple2<>("update",0);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) {
                return integer1 + integer2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._1 + "," + stringIntegerTuple2._2,"");
            }
        }).repartition(1).saveAsHadoopFiles(ConfigLoader.getProperties(UserConstants.SPARK_MINUTE_OUTPUT), DateUntil.getDateOneMinuteAgoSecond(),String.class,NullWritable.class, RDDMultipleTextOutputFormat.class);
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }

    @Override
    public void execute(String[] args){

    }
    public static void main(String[] args) {
        Channel_Promotion_Count_v2 channel_promotion_count = new Channel_Promotion_Count_v2();
        channel_promotion_count.execute();
    }
}
