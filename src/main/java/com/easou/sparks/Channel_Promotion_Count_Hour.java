package com.easou.sparks;

import com.easou.Constants.UserConstants;
import com.easou.common.AbstractCommonHelper;
import com.easou.interfaces.BIModel;
import com.easou.spark.userrddmultipleyextoutputformat.RDDMultipleTextOutputFormat_Hour;
import com.easou.untils.ConfigLoader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * ClassName Channel_Promotion_Count_Hour
 * 功能: mobile_info 日志 按分钟每个渠道新增用户数小时级别数据汇总
 * Author yangweifeng
 * Date 2018/11/23 17:20
 * Version 1.0
 **/
public class Channel_Promotion_Count_Hour extends AbstractCommonHelper implements BIModel {
    private static final long serialVersionUID = 2L;
    @Override
    public void execute(final String[] args) {
       // SparkConf sparkConf = new SparkConf().setAppName("Channel_Promotion_Count_Hour").setMaster("local");
        SparkConf sparkConf = new SparkConf().setAppName("Channel_Promotion_Count_Hour");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = javaSparkContext.textFile(ConfigLoader.getProperties(UserConstants.SPARK_MINUTE_OUTPUT),5);
       // JavaRDD<String> lines = javaSparkContext.textFile("F:\\data",1);
        lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line)  {
                String values [] = line.split("\\s+");
                // 以结束时间为准
                if (values.length == 7 && values[5].contains(args[0])){
                  return true;
                }
               return false;
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) {
                 String values [] = line.split("\\s+");
                 StringBuffer stringBuffer = new StringBuffer();
                 stringBuffer.append(values[0]).append("\t").append(values[1]).append("\t").append(values[2]).append("\t").append(values[3]).append("\t").append(args[0]);
                 return new Tuple2<>(stringBuffer.toString(),Integer.parseInt(values[6]));
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer num1, Integer num2){
                return num1 + num2;
            }
        }).repartition(1).saveAsHadoopFile(ConfigLoader.getProperties(UserConstants.SPARK_HOUR_OUTPUT) + args[0].substring(0,8) , Text.class, IntWritable.class, RDDMultipleTextOutputFormat_Hour.class);
        javaSparkContext.close();
    }

    @Override
    public void execute()  {

    }

    public static void main(String[] args){
        if (args.length != 1){
            System.out.println("请传递小时参数: demo 2018112212");
            return;
        }
        final String[] paramrs = args;
        Channel_Promotion_Count_Hour channel_promotion_count_hour = new Channel_Promotion_Count_Hour();
        channel_promotion_count_hour.execute(paramrs);
    }
}
