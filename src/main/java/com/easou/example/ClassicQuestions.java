package com.easou.example;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * ClassName ClassicQuestions
 * 功能: 经典问题
 * Author yangweifeng
 * Date 2018/11/25 13:17
 * Version 1.0
 **/
public class ClassicQuestions implements Serializable {
    private static final long serialVersionUID = 1L;
    public static void main(String[] args) {
        ClassicQuestions classicQuestions = new ClassicQuestions();
        SparkConf sparkConf = new SparkConf().setAppName("accumulator");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        classicQuestions.accumulator(javaSparkContext);
    }
    /**
     * 二次排序
     */
    private void SecondSortAction(JavaSparkContext javaSparkContext){
        JavaRDD<String> lines = javaSparkContext.textFile("E:\\data2");
        JavaPairRDD<SecondSortKey,String> pairs = lines.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String line) {
                String values[] = line.split("\\s+");
                SecondSortKey secondSortKey = new SecondSortKey(Integer.parseInt(values[0]),Integer.parseInt(values[1]));
                // 根据key排序这一行
                return new Tuple2<>(secondSortKey,line);
            }
        });
        JavaPairRDD<SecondSortKey,String> pairsSort = pairs.sortByKey();
        JavaRDD<String> res = pairsSort.map(new Function<Tuple2<SecondSortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondSortKey, String> secondSortKeyStringTuple2) throws Exception {
                return secondSortKeyStringTuple2._2;
            }
        });
        res.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) {
                System.out.println(line);
            }
        });
        javaSparkContext.close();
    }

    /**
     * 累加变量
     */
    private  void accumulator(JavaSparkContext javaSparkContext){
        // 定义累加变量
        final Accumulator<Integer> sum = javaSparkContext.accumulator(0);
        List<Integer> list = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> listRdd = javaSparkContext.parallelize(list);
        listRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num) throws Exception {
                sum.add(num);
            }
        });
        System.out.println(sum.value());
        javaSparkContext.close();
    }

    /**
     * 排序版单词计数
     */
    private  void sortWordCount(JavaSparkContext javaSparkContext){
        JavaRDD<String> lines = javaSparkContext.textFile("E:\\data");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line)  {
                return Arrays.asList(line.split("\\s+"));
            }
        });
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s,1);
            }
        });
        JavaPairRDD<String,Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });
        /*
          排序
          sort
          sortbykey
         */
         // 进行key value 反转
        JavaPairRDD<Integer,String> wordCountF = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2,stringIntegerTuple2._1);
            }
        });
        // 可以定义key的比较器 作为参数
        JavaPairRDD<Integer,String> wordCountFSort = wordCountF.sortByKey();
        // 也可以再次反转映射回去
        wordCountFSort.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> stringIntegerTuple2) {
                System.out.println(stringIntegerTuple2._2 + "," +stringIntegerTuple2._1);
            }
        });
        javaSparkContext.close();
    }

    /**
     * 分组groupby 取top
     */
    private void Top3Group(JavaSparkContext javaSparkContext){
        JavaRDD<String> lines = javaSparkContext.textFile("E:\\data4");
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String [] values = s.split("\\s+");
                return new Tuple2<>(values[0],Integer.parseInt(values[1]));
            }
        });
        //key值相同的分到一组
        JavaPairRDD<String,Iterable<Integer>> pairsGroup = pairs.groupByKey();
        JavaPairRDD<String,Iterable<Integer>> pairsGroupTop3 = pairsGroup.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) {
                String className = stringIterableTuple2._1;
                //迭代比较大小 或者先排序 取前三位
                /*
                List<Integer> list = new ArrayList<>();
                Iterator<Integer> srores = stringIterableTuple2._2.iterator();
                while (srores.hasNext()){
                   Integer srore = srores.next();
                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null){
                            top3[i] = srore;
                            break;
                        }else if(srore > top3[i]){
                            for (int j = 2; j > i; j--) {
                                top3[j] = top3[j - i];
                            }
                            top3[i] = srore;
                            break;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
                */
                // 方法2
                List<Integer> list = IteratorUtils.toList(stringIterableTuple2._2.iterator());
                Collections.sort(list, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });
                Integer [] top3 = new Integer[3];
                for (int i = 0; i < 3; i++) {
                    top3[i] = list.get(i);
                }
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });

        pairsGroupTop3.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) {
                Iterator<Integer> top3 = stringIterableTuple2._2.iterator();
                StringBuffer stringBuffer = new StringBuffer(stringIterableTuple2._1).append(":");
                while (top3.hasNext()){
                    stringBuffer.append(top3.next()).append(",");
                }
                System.out.println(stringBuffer.toString());
            }
        });


    }

    /**
     * top 3
     * @param javaSparkContext JavaSparkContext
     */
    private void Top3(JavaSparkContext javaSparkContext){
        JavaRDD<String> lines = javaSparkContext.textFile("E:\\data3");
        JavaRDD<Integer> numbers = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                try {
                    return Integer.parseInt(s);
                }catch (Exception e){
                    return 0;
                }
            }
        });
        JavaPairRDD<Integer,Integer> numbersSort = numbers.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer,new Integer(0));
            }
        });
        JavaPairRDD<Integer,Integer> numbersSortpars = numbersSort.sortByKey(false);
        JavaRDD<Integer> numbersSort1 = numbersSortpars.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return integerIntegerTuple2._1;
            }
        });
        List<Integer> top3 = numbersSort1.take(3);
        System.out.println(top3);
    }
}
